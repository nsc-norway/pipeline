# Generate reports after FastQC has completed

# This task requires an intermediate amount of CPU resources. Will be 
# executed on server handling the scripts.

import os
import time

from common import nsc, remote, taskmgr

TASK_NAME = "70. MultiQC"
TASK_DESCRIPTION = """Runs the MultiQC tool to aggregate FastQC results."""

TASK_ARGS = ['work_dir', 'sample_sheet', 'threads', 'lanes']


def main(task):
    task.running()
    bc_dir = task.bc_dir
    projects = task.projects

    # Per-project fastqc multiqc reports
    output_dir = os.path.join(bc_dir, "QualityControl" + task.suffix)
    commands = []
    for project in projects:
        if not project.is_undetermined:
            project_qc_dir = os.path.join(output_dir, project.name)
            commands.append(nsc.MULTIQC + ["-q", "-f", "-o", project_qc_dir, project_qc_dir])

    mqc = remote.ArrayJob(commands, "multiqc", "01:00:00", task.logfile("multiqc.%a"))
    mqc.mem_per_task = 8000
    mqc.cpus_per_task = 1
    mqc.comment = task.run_id
    remote.ArrayJob.start_jobs([mqc], max_local_threads=task.threads)
    remote.ArrayJob.update_status([mqc])
    task.array_job_status([mqc])

    while not mqc.is_finished:
        time.sleep(10)
        remote.ArrayJob.update_status([mqc])
        task.array_job_status([mqc])

    if mqc.summary.keys() != ["COMPLETED"]:
        task.fail("MultiQC failed (check logs)")

    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)


