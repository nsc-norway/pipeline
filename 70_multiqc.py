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

    for project in projects:
        remote.run_command()

    task.success_finish()


def run_multiqc(bc_dir, project):
    pass



if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)


