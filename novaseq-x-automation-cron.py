import yaml
import datetime
import sys
from pathlib import Path
import subprocess
import os
import logging


DEMULTIPLEXED_RUNS_PATH = Path("/data/runScratch.boston/demultiplexed")
SCRIPT_DIR_PATH = Path(__file__).resolve().parent
INPUT_RUN_PATH = Path("/data/runScratch.boston/NovaSeqX")


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


def setup_logging(analysis_path):
    # Set up separate loggers for progress and errors
    logfile_path = analysis_path / "automation_log_nsc.txt"
    
    # Progress logger (logs only to file)
    progress_logger = logging.getLogger(f"progress_{analysis_path}")
    progress_logger.setLevel(logging.INFO)
    
    file_handler = logging.FileHandler(logfile_path)
    file_formatter = logging.Formatter('%(asctime)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    # Only log progress information to the file
    progress_logger.addHandler(file_handler)
    
    # Exception logger (logs to both file and stderr)
    error_logger = logging.getLogger(f"error_{analysis_path}")
    error_logger.setLevel(logging.ERROR)
    
    # Console handler for stderr (for sending emails)
    console_handler = logging.StreamHandler(sys.stderr)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
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

                demultiplexed_run_dir = DEMULTIPLEXED_RUNS_PATH / run_id
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
                any_nondiag_project = False
                for project_name in projects:
                    progress_logger.info(f"Processing project {project_name}")
                    project_samples = [sample for sample in lims_info['samples'] if sample['project_name'] == project_name]
                    project_type = project_samples[0]['project_type']
                    delivery_method = project_samples[0]['delivery_method'].replace(" ", "_")
                    if project_type == "Diagnostics":
                        any_diag_project = True
                    else:
                        any_nondiag_project = True
                        run_fastqc = "false" if lims_info.get("compute_platform") == "Onboard DRAGEN" else "true"
                        job_id = start_nsc_nextflow(project_name, run_id, suffix, delivery_method, demultiplexed_run_dir, run_fastqc, bcl_convert_version)
                        nsc_project_slurm_jobs.append(job_id)

                # Queue run-based processing
                if nsc_project_slurm_jobs:
                    run_slurm_script = f"""#!/bin/bash
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --job-name=run-{run_id}
#SBATCH --time=1-0
#SBATCH --qos=high

/boston/common/tools/nextflow/nextflow-23.10.1-all run /data/runScratch.boston/analysis/pipelines/novaseqx_nextflow/main_run.nf \\
    --runid "{run_id}" \\
    --qcid "QualityControl{suffix}" \\
    --analysisid "Analysis{suffix}" \\
    --samplerenaminglist "SampleRenamingList-run.csv" \\
    --bcl_convert_version "{bcl_convert_version}"
"""
                    dependency_list = "afterany:" + ":".join(nsc_project_slurm_jobs)
                    pipeline_dir = demultiplexed_run_dir / "pipeline" / "run"
                    pipeline_dir.mkdir(parents=True, exist_ok=True)
                    slurm_script_path = pipeline_dir / "script.sh"
                    with open(slurm_script_path, 'w') as slurm_script_file:
                        slurm_script_file.write(run_slurm_script)
                    run_subprocess_with_logging(
                        error_logger,
                        ["sbatch", "--dependency=" + dependency_list, str(slurm_script_path)],
                        cwd=pipeline_dir,
                    )

                if any_diag_project:
                    run_subprocess_with_logging(
                        error_logger,
                        ["nsc-python3", str(SCRIPT_DIR_PATH / "novaseq-x-diag.py"), str(lims_file_path)],
                    )
                if not any_nondiag_project:
                    # TODO - close the sequencing step for diag-only runs
                    pass

                progress_logger.info(f"Completed automation at {datetime.datetime.now()}")
    
            except Exception as e:
                error_logger.error(f"Exception occurred: {e}", exc_info=True)
                raise  # Re-raise the exception to trigger any necessary stderr email output


def start_nsc_nextflow(project_name, run_id, suffix, delivery_method, demultiplexed_run_dir, run_fastqc, bcl_convert_version):
    slurm_script = f"""#!/bin/bash
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --job-name=prj-{project_name}
#SBATCH --time=3-0
#SBATCH --qos=high

/boston/common/tools/nextflow/nextflow-23.10.1-all run /data/runScratch.boston/analysis/pipelines/novaseqx_nextflow/main_project.nf \\
    --runid "{run_id}" \\
    --qcid "QualityControl{suffix}" \\
    --analysisid "Analysis{suffix}" \\
    --samplerenaminglist "SampleRenamingList-{project_name}.csv" \\
    --deliverymethod "{delivery_method}" \\
    --run_fastqc {run_fastqc} \\
    --bcl_convert_version "{bcl_convert_version}"
"""
    pipeline_dir = demultiplexed_run_dir / "pipeline" / ("prj-" + project_name)
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    slurm_script_path = pipeline_dir / "script.sh"
    with open(slurm_script_path, 'w') as slurm_script_file:
        slurm_script_file.write(slurm_script)

    job_id = subprocess.run(
        ["sbatch", "--parsable", str(slurm_script_path)],
        cwd=pipeline_dir,
        check=True,
        stdout=subprocess.PIPE).stdout.decode().strip()
    
    return job_id

if __name__ == "__main__":
    main()
