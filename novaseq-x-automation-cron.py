import yaml
import datetime
import sys
from pathlib import Path
import subprocess
import os

def main():
    os.umask(0o007)
    input_run_path = Path("/data/runScratch.boston/NovaSeqX")
    demultiplexed_runs_path = Path("/data/runScratch.boston/demultiplexed")
    script_dir_path = Path(__file__).resolve().parent

    for lims_file_path in input_run_path.glob("*/Analysis/*/ClarityLIMSImport_NSC.yaml"):
        analysis_path = lims_file_path.parents[0]
        run_id = lims_file_path.parents[2].name

        automation_log_path = analysis_path / "automation_log_nsc.txt"
        if not automation_log_path.is_file():
            # Run automation for this path
            
            # Open the LIMS file
            with open(lims_file_path) as f:
                lims_info = yaml.safe_load(f)

            if lims_info.get('status') != 'ImportCompleted':
                continue

            # Misc. setup
            logfile = open(automation_log_path, 'w')
            print("Started automation at", str(datetime.datetime.now()), file=logfile)
            
            demultiplexed_run_dir = demultiplexed_runs_path / run_id # Created by file mover if nsc projects
            # Suffix for some directories created by the file mover script
            if analysis_path.name == "1":
                suffix = ""
            else:
                suffix = f"_{analysis_path.name}"

            # Run the file mover synchronously
            subprocess.run(
                ["nsc-python3", str(script_dir_path / "novaseq-x-file-mover.py"), str(analysis_path)],
                check=True
            )

            # Project-specific processing
            projects = set(sample['project_name'] for sample in lims_info['samples'])
            nsc_project_slurm_jobs = []
            any_diag_project = False
            for project_name in projects:
                print("Processing project", project_name, file=logfile)
                project_samples = [sample for sample in lims_info['samples'] if sample['project_name'] == project_name]
                project_type = project_samples[0]['project_type']
                delivery_method = project_samples[0]['delivery_method'].replace(" ", "_")
                if project_type == "Diagnostics":
                    any_diag_project = True
                else:
                    job_id = start_nsc_nextflow(project_name, run_id, suffix, delivery_method, demultiplexed_run_dir)
                    nsc_project_slurm_jobs.append(job_id)

            # Queue run-based processing if there are any NSC projects
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
    --samplerenaminglist "SampleRenamingList-run.csv" 
"""
                dependency_list = "afterany:" + ":".join(nsc_project_slurm_jobs)
                pipeline_dir = demultiplexed_run_dir / "pipeline" / "run"
                pipeline_dir.mkdir(parents=True, exist_ok=True)
                slurm_script_path = pipeline_dir / "script.sh"
                with open(slurm_script_path, 'w') as slurm_script_file:
                    slurm_script_file.write(run_slurm_script)
                subprocess.run(
                        ["sbatch", "--dependency=" + dependency_list, str(slurm_script_path)],
                        cwd=pipeline_dir,
                        check=True,
                        stdout=subprocess.DEVNULL
                )

            # Finally run the diagnostics processing job. Done last because this is blocking on the md5sum jobs.
            if any_diag_project:
                subprocess.run(
                    ["nsc-python3", str(script_dir_path / "novaseq-x-diag.py"), str(lims_file_path)],
                    check=True
                )

            print("Completed automation at", str(datetime.datetime.now()), file=logfile)


def start_nsc_nextflow(project_name, run_id, suffix, delivery_method, demultiplexed_run_dir):
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
    --deliverymethod "{delivery_method}"
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
