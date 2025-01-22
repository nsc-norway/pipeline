import os
import yaml
import sys
import re
import asyncio
import time
from collections import defaultdict
from pathlib import Path
from genologics.lims import *
from genologics import config

DIAG_DESTINATION_PATH = Path("/boston/diag/nscDelivery")
DIAG_RUN_FOLDER_MOVE_PATH = Path("/boston/diag/runs")
lims = Lims(config.BASEURI, config.USERNAME, config.PASSWORD)

async def process_all_diag_projects(lims_file_path):
    """
    Compute md5sums and then add to lims workflow.
    Process all diagnostics projects in the specified yaml file.
    NOTE: The lims file has to exist in its original location. The path will be used to determine
    the Run ID.
    """


    with open(lims_file_path) as f:
        lims_info = yaml.safe_load(f)

    run_folder = lims_file_path.resolve().parents[2]
    run_id = run_folder.name
    analysis_id = lims_file_path.resolve().parents[0].name

    samples = lims_info['samples']

    # Check sample info
    project_md5sum_results_async = {}
    project_fastq_dirs = {}
    have_any_nsc_samples = False
    for sample in samples:
        if sample['project_type'] == "Diagnostics":
            project = sample['project_name']
            if project not in project_fastq_dirs:
                project_fastq_dirs[project] = get_project_fastq_dir_path(run_id, sample)
                project_md5sum_results_async[project] = list()
            fastq_dir = project_fastq_dirs[project]
            for fastq_name in get_fastq_names(sample):
                project_md5sum_results_async[project].append(run_md5sum(fastq_dir, fastq_name))
        elif sample['project_type'] in ["Sensitive", "Non-sensitive"]:
            have_any_nsc_samples = True

    for project, fastq_dir in project_fastq_dirs.items():
        md5sum_results = await asyncio.gather(*project_md5sum_results_async[project])
        assert all(exit_code == 0 for exit_code, stdout in md5sum_results), "all processes should have zero exit code"
        with open(fastq_dir / "md5sum.txt", "wb") as md5sum_file:
            for _, stdout in md5sum_results:
                md5sum_file.write(stdout)

    # Add to LIMS workflow
    # 1. find newest version of bioinformatics workflow
    workflows = lims.get_workflows()
    match_workflows = [] # Contains version, then workflow object
    for w in workflows:
        # This will do a GET for each workflow in the system. Performance is bad.
        m = re.match(r"processing of hts-data diag (\d)\.(\d)", w.name, re.IGNORECASE)
        if w.status == "ACTIVE" and m:
            match_workflows.append((int(m.group(1)), int(m.group(2)), w))
    major, minor, workflow = sorted(match_workflows)[-1]
    # 2. find root artifacts of the samples in this analysis
    artifacts = [Sample(lims, id=sample['sample_id']).artifact for sample in samples if sample['project_type'] == "Diagnostics"]
    # 3. queue artifacts
    lims.route_analytes(artifacts, workflow)

    if not have_any_nsc_samples:
        # Complete the sequencing QC step
        lane_artifact_ids = list(set(sample['lane_artifact'] for sample in samples))
        qc_pass_and_complete_seq_step(lane_artifact_ids)

        # Move the run folder only if this is analysis ID 1 - first time demultiplexing
        if analysis_id == "1":
            dest_path = DIAG_RUN_FOLDER_MOVE_PATH / run_folder.name
            if not dest_path.exists():
                run_folder.rename(DIAG_RUN_FOLDER_MOVE_PATH / run_folder.name)


def qc_pass_and_complete_seq_step(lane_artifact_ids):
    """Pass and close the QC step."""

    processes = lims.get_processes(inputartifactlimsid=lane_artifact_ids)
    # Find process that is not completed and has "per input" output generation
    # This will be the QC process
    is_per_input = False
    for p in processes:
        if not p.date_run and 'NovaSeq X Run QC' in p.type_name:
            for i, o in p.input_output_maps:
                if o['output-generation-type'] == "PerInput":
                    is_per_input = True
                    break
    if is_per_input:
        for i, o in p.input_output_maps:
            if o['output-generation-type'] == "PerInput":
                o['uri'].get()
                o['uri'].qc_flag = "PASSED"
                o['uri'].put()
        step = Step(lims, id=p.id)
        for na in step.actions.next_actions:
            na['action'] = 'complete'
        step.actions.put()
        attempt = 0
        while attempt < 10 and step.current_state.upper() != "COMPLETED":
            step.advance()
            time.sleep(10)
            step.get(force=True)
            attempt += 1


async def run_md5sum(fastq_dir, filename):
    proc = await asyncio.create_subprocess_exec(
            "srun", "--qos=high", "md5sum", filename,
            cwd=fastq_dir,
            stdout=asyncio.subprocess.PIPE
            )
    stdout, stderr = await proc.communicate()
    return proc.returncode, stdout


def get_fastq_names(sample):
    """Determines the paths of the files."""

    compression_type = "ora" if sample.get('ora_compression') else "gz"
    return ["_".join([
                        sample['samplesheet_sample_id'],
                        f"S{sample['samplesheet_position']}",
                        "L" + str(sample['lane']).zfill(3),
                        "R" + str(read_nr),
                        f"001.fastq.{compression_type}"
                    ])
                    for read_nr in range(1, (sample['num_data_read_passes'] + 1))
    ]


def get_project_fastq_dir_path(run_id, sample):
    """Return a Path pointing to the directory for FASTQ files for this sample."""

    #fastq_dir = "ora_fastq" if sample.get('ora_compression') else "fastq"
    # The fastq location is always "fastq" even if ora is used.
    return DIAG_DESTINATION_PATH / get_project_dir_name(run_id, sample) / "fastq"


def get_project_dir_name(run_id, sample):
    """Get long project directory name (common NSC format).
    
    YYMMDD_LH00534.A.Project_Diag-wgs123-2029-01-01"""

    runid_parts = run_id.split("_")
    flowcell_side = runid_parts[-1][0] # A or B
    # Trim off the first two year digits
    date = runid_parts[0][2:]
    serial_no = runid_parts[1]
    return f"{date}_{serial_no}.{flowcell_side}.Project_{sample['project_name']}"


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("use: novaseq-x-diag.py LIMS_FILE")
        sys.exit(1)

    lims_file = Path(sys.argv[1])
    if not lims_file.is_file():
        print("error: LIMS file", lims_file, "does not exist.")
        sys.exit(1)

    asyncio.run(process_all_diag_projects(lims_file))

