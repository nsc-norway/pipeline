# Generate reports after FastQC has completed

# This task requires an intermediate amount of CPU resources. Will be 
# executed on server handling the scripts.

import sys
import os
import re
import shutil
import multiprocessing
import subprocess
from xml.etree import ElementTree

from common import nsc, stats, samples, utilities, taskmgr

TASK_NAME = "70. MultiQC"
TASK_DESCRIPTION = """Runs the MultiQC tool to aggregate FastQC results."""

TASK_ARGS = ['work_dir', 'sample_sheet', 'lanes']


def main(task):
    task.running()
    bc_dir = task.bc_dir
    projects = task.projects

    # Create for bcl2fastq stats only
    subprocess.call(nsc.MULTIQC + ["-q", "-f", "Stats/"], cwd=bc_dir)

    # Per-project fastqc multiqc reports
    output_dir = os.path.join(bc_dir, "QualityControl" + task.suffix)

    for project in projects:
        project_qc_dir = os.path.join(output_dir, project.name or "Undetermined")
        subprocess.call(nsc.MULTIQC + ["-q", "-f", "."], cwd=project_qc_dir)

    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)


