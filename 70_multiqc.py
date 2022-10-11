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
    qc_dir = os.path.join(bc_dir, "QualityControl" + task.suffix)
    commands = []
    for project in projects:
        if not project.is_undetermined:
            project_qc_dir = project.name
            commands.append(nsc.MULTIQC + ["-m", "fastqc", "-q", "-f", "-o", project_qc_dir, project_qc_dir])

    mqc = remote.ArrayJob(commands, "multiqc", "02:00:00", task.logfile("multiqc.%a"))
    mqc.cwd = qc_dir
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

    # Make links to multiqc reports in the delivery dir
    for project in projects:
        if project.is_undetermined: continue

        link_placement =  "{}/Delivery/email_content/{}_multiqc.html".format(qc_dir, project.proj_dir)
        source = "{}/{}/multiqc_report.html".format(qc_dir, project.name)
        if os.path.isfile(source):
            try:
                os.link(source, link_placement)
            except OSError as e:
                if e.errno == 17: pass # File exists
                else: raise

    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)


