import yaml
import datetime
import sys
from pathlib import Path
import subprocess
import os
import logging


# Test mode disables the calls to move files
TEST_MODE = os.environ.get("TEST_MODE", "False").lower() in ("1", "true", "yes")

if TEST_MODE:
    NSC_DEMULTIPLEXED_RUNS_PATH = Path("test/nsc")
    NSC_DEMULTIPLEXED_RUNS_PATH.mkdir(exist_ok=True, parents=True)
    MIK_PATH = Path("test/nsc")
    MIK_PATH.mkdir(exist_ok=True, parents=True)
    INPUT_RUN_PATH = Path(".")
else:
    NSC_DEMULTIPLEXED_RUNS_PATH = Path("/data/runScratch.boston/demultiplexed")
    MIK_PATH = Path("/data/runScratch.boston/mik_data")
    INPUT_RUN_PATH = Path("/data/runScratch.boston/NovaSeqX")


SCRIPT_DIR_PATH = Path(__file__).resolve().parent


def run_subprocess_with_logging(error_logger, args, **kwargs):
    result = subprocess.run(args, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True, **kwargs)
    if result.returncode != 0:
        error_logger.error(f"Command '{' '.join(args)}' failed with exit code {result.returncode}.")
        if result.stderr:
            error_logger.error("BEGIN STDERR:\n" + result.stderr)
            error_logger.error("END STDERR")
        if result.stdout:
            error_logger.error("BEGIN STDOUT:\n" + result.stdout)
            error_logger.error("END STDOUT")
        raise RuntimeError("Called process error")


def setup_logging(analysis_path):
    # Set up separate loggers for progress and errors
    logfile_path = analysis_path / "automation_log_nsc.txt"
    

    # Setup logging handlers
    # Console handler for stderr (for sending emails / console output)
    console_handler = logging.StreamHandler(sys.stderr)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    # File logging
    file_handler = logging.FileHandler(logfile_path)
    file_formatter = logging.Formatter('%(asctime)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Progress logger logs only to file unless TEST_MODE
    progress_logger = logging.getLogger(f"progress_{analysis_path}")
    progress_logger.setLevel(logging.INFO)
    
    # Only log progress information to the file
    progress_logger.addHandler(file_handler)
    if TEST_MODE:
        progress_logger.addHandler(console_handler)
    
    # Exception logger (logs to both file and stderr)
    error_logger = logging.getLogger(f"error_{analysis_path}")
    error_logger.setLevel(logging.ERROR)
    
    # Reuse the file handler for error logger (log to both stderr and file)
    error_logger.addHandler(file_handler)
    error_logger.addHandler(console_handler)
    
    return progress_logger, error_logger

def main():
    os.umask(0o007)

    for lims_file_path in INPUT_RUN_PATH.glob("*/Analysis/*/ClarityLIMSImport_NSC.yaml"):
        analysis_path = lims_file_path.parents[0]
        run_id = lims_file_path.parents[2].name

        automation_log_path = analysis_path / "automation_log_nsc.txt"
        if not automation_log_path.is_file():
            # Run automation for this path
            
            # Open the LIMS file
            with open(lims_file_path) as f:
                lims_info = yaml.safe_load(f)

            if lims_info.get('status') != 'ImportCompleted':
                continue # If the LIMS import is not completed, we skip this path.

            bcl_convert_version = lims_info.get("bcl_convert_version", "UNKNOWN")

            # Setup the loggers, also creating the log file, which is used as a flag to not rerun the automation.
            progress_logger, error_logger = setup_logging(analysis_path)
            try:
                # Log progress
                progress_logger.info(f"Started automation at {datetime.datetime.now()}")

                if analysis_path.name == "1":
                    suffix = ""
                else:
                    suffix = f"_{analysis_path.name}"

                # Run the file mover
                run_subprocess_with_logging(
                    error_logger,
                    ["nsc-python3", str(SCRIPT_DIR_PATH / "novaseq-x-file-mover.py"), str(analysis_path)],
                )

                # Project-specific processing
                projects = set(sample['project_name'] for sample in lims_info['samples'])
                nsc_project_slurm_jobs = []
                any_diag_project = False
                for project_name in projects:
                    project_samples = [sample for sample in lims_info['samples'] if sample['project_name'] == project_name]
                    project_type = project_samples[0]['project_type']
                    progress_logger.info(f"Processing project {project_name} of type {project_type}.")
                    delivery_method = project_samples[0]['delivery_method'].replace(" ", "_")
                    if project_type == "Diagnostics":
                        any_diag_project = True
                    elif project_type in ["Sensitive", "Non-Sensitive"]: # NSC
                        is_onboard = "false" if lims_info.get("compute_platform") == "Onboard DRAGEN" else "true"
                        is_paired_end = project_samples[0]['num_data_read_passes'] == 2
                        is_ora = project_samples[0]['ora_compression']
                        demultiplexed_run_dir = NSC_DEMULTIPLEXED_RUNS_PATH / run_id
                        job_id = start_nsc_nextflow(project_name, run_id, suffix, delivery_method, demultiplexed_run_dir, is_paired_end, is_onboard, is_ora, bcl_convert_version)
                        nsc_project_slurm_jobs.append(job_id)
                    elif project_type == "Microbiology":
                        start_human_removal(run_id, project_name, project_samples)
                    elif project_type == "PGT":
                        progress_logger.info(f"No additional actions required for PGT project {project_name}.")
                    else:
                        progress_logger.info(f"Unknown project type {project_type} for project {project_name}. Skipping.")

                # Queue run-based processing
                if nsc_project_slurm_jobs:
                    progress_logger.info(f"Submitting run-level NSC job.")
                    run_slurm_script = f"""#!/bin/bash
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --job-name=run-{run_id}
#SBATCH --time=1-0
#SBATCH --qos=high

/boston/common/tools/nextflow/nextflow-23.10.1-all run /data/runScratch.boston/analysis/pipelines/novaseqx_nextflow/main_run.nf \\
    --runid "{run_id}" \\
    --runFolder "{demultiplexed_run_dir}" \\
    --qcid "QualityControl{suffix}" \\
    --analysisid "Analysis{suffix}" \\
    --bclConvertVersion "{bcl_convert_version}"
"""
                    dependency_list = "afterany:" + ":".join(nsc_project_slurm_jobs)
                    pipeline_dir = demultiplexed_run_dir / "pipeline" / "run"
                    pipeline_dir.mkdir(parents=True, exist_ok=True)
                    slurm_script_path = pipeline_dir / f"script{suffix}.sh"
                    with open(slurm_script_path, 'w') as slurm_script_file:
                        slurm_script_file.write(run_slurm_script)
                    run_subprocess_with_logging(
                        error_logger,
                        ["sbatch", "--dependency=" + dependency_list, str(slurm_script_path)],
                        cwd=pipeline_dir,
                    )

                if any_diag_project:
                    progress_logger.info(f"Calling diagnostics automation script.")
                    run_subprocess_with_logging(
                        error_logger,
                        ["nsc-python3", str(SCRIPT_DIR_PATH / "novaseq-x-diag.py"), str(lims_file_path)],
                    )

                progress_logger.info(f"Completed automation at {datetime.datetime.now()}")
    
            except Exception as e:
                error_logger.error(f"Exception occurred: {e}", exc_info=True)
                raise  # Re-raise the exception to trigger any necessary stderr email output


def start_nsc_nextflow(project_name, run_id, suffix, delivery_method, demultiplexed_run_dir, is_onboard, bcl_convert_version):
    """Start project level automation script"""

    slurm_script = f"""#!/bin/bash
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --job-name=prj-{project_name}
#SBATCH --time=3-0
#SBATCH --qos=high

/boston/common/tools/nextflow/nextflow-23.10.1-all run /data/runScratch.boston/analysis/pipelines/novaseqx_nextflow/main_project.nf \\
    --runid "{run_id}" \\
    --runFolder "${demultiplexed_run_dir} \\
    --qcid "QualityControl{suffix}" \\
    --analysisid "Analysis{suffix}" \\
    --project "{project_name}" \\
    --enableFastQC {not is_onboard} \\
    --deliveryMethod {delivery_method} \\
    --bclConvertVersion "{bcl_convert_version}"
"""
    pipeline_dir = demultiplexed_run_dir / "pipeline" / ("prj-" + project_name)
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    slurm_script_path = pipeline_dir / f"script{suffix}.sh"
    with open(slurm_script_path, 'w') as slurm_script_file:
        slurm_script_file.write(slurm_script)

    job_id = subprocess.run(
        ["sbatch", "--parsable", str(slurm_script_path)],
        cwd=pipeline_dir,
        check=True,
        stdout=subprocess.PIPE).stdout.decode().strip()
    
    return job_id


def dir_name(project_name, run_id):
    """Get standard project directory name"""

    runid_parts = run_id.split('_')
    date6 = runid_parts[0][2:]
    serial = runid_parts[1]
    side = runid_parts[-1][0]
    return f"{date6}_{serial}.{side}.Project_{project_name}"


def start_human_removal(run_id, project_name, project_samples):
    project_dir = MIK_PATH / dir_name(project_name, run_id)
    script_path = "/data/runScratch.boston/mik_data/human_cleanup_analysis/mik_cleanup_script.sh"
    for sample in project_samples:
        # Start one human removal job per sample
        mik_sample_id = f"{sample['sample_name']}_S{sample['samplesheet_position']}_L{str(sample['lane']).zfill(3)}"
        subprocess.run(
            ["sbatch", str(script_path), mik_sample_id],
            cwd=project_dir,
            check=True
        )
        # The command will fail if job submission fails, but pipeline failures are not fatal here,
        # will be logged in the script logs.

if __name__ == "__main__":
    main()

