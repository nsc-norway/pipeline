#!/bin/env python

# Generate checksum for fastq files and pdf files

TASK_NAME = "Checksums"
TASK_DESCRIPTION = """Generates HTML and PDF reports based on demultiplexing stats
                    and FastQC results."""

TASK_ARGS = ['work_dir', 'sample_sheet', 'threads']

def main(task):
    pass


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)
