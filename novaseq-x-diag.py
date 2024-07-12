import os
import yaml
import sys
import asyncio
from collections import defaultdict
from pathlib import Path
from genologics.lims import *
from genologics import config

async def process_all_diag_projects(lims_file_path):
    """
    Compute md5sums and then add to lims workflow.
    Process all diagnostics projects in the specified yaml file.
    NOTE: The lims file has to exist in its original location. The path will be used to determine
    the Run ID.
    """
    input_run_dir = analysis_path.resolve().parents[2]
    run_id = input_run_dir.name

    # Load LIMS-based information
    lims_file_path = analysis_dir / "ClarityLIMSImport_NSC.yaml"
    with open(lims_file_path) as f:
        lims_info = yaml.safe_load(f)

    samples = lims_info['samples']

    # Check sample info
    project_md5sum_results_async = {}
    project_fastq_dirs = {}
    for sample in samples:
        if sample['project_type'] == "Diagnostics":
            project = sample['project_name']
            if project not in project_fastq_dirs:
                project_fastq_dirs[project] = get_project_fastq_dir_path(run_id, sample)
                project_md5sum_results_async[project] = list()
            fastq_dir = project_fastq_dirs[project]
            for fastq_name in get_fastq_names(sample):
                project_md5sum_results_async[project].append(run_md5sum(fastq_dir, fastq_name))

    for project, fastq_dir in project_fastq_dir.items():
        md5sum_results = await asyncio.gather(*project_md5sum_results_async)
        assert all(exit_code == 0 for exit_code, stdout in md5sum_results), "all processes should have zero exit code"
        with open(fastq_dir / "md5sum.txt", "w") as md5sum_file:
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
    artifacts = [Sample(lims, sample['sample_id']).artifact for sample in samples if sample['project_type'] == "Diagnostics"]
    # 3. queue artifacts
    lims.route_analytes(artifact, workflow)


async def run_md5sum(fastq_dir, filename):
    proc = await asyncio.create_subprocess_exec(
            ["srun", "md5sum", filename],
            cwd=fastq_dir,
            stdout=asyncio.subprocess.PIPE
            )
    stdout = await proc.communicate()
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
    if not analysis_dir.is_file():
        print("error: LIMS file", lims_file, "does not exist.")
        sys.exit(1)

    asyncio.run(process_all_diag_projects(analysis_dir))


