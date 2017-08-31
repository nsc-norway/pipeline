import os
from common import taskmgr, samples, nsc, remote

TASK_NAME = "40. Copy SAV files"
TASK_DESCRIPTION = """Copy SAV files for diagnostics group."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'lanes']

SAV_REPOSITORY_DIR = "/data/runScratch.boston/test"

# NextSeq: RunInfo.xml, RunParameters.xml
# MiSeq, HiSeq2500, HiSeq3000, HiSeqX: RunInfo.xml, runParameters.xml

SAV_INCLUDE_PATHS = [
        "RunInfo.xml",
        "runParameters.xml",
        "RunParameters.xml",
        "InterOp",
        "Data/Intensities/BaseCalls/reports"
        ]


def main(task):
    task.running()
    run_id = task.run_id
    work_dir = task.work_dir
    projects = task.projects

    if any(project.name.startswith("Diag-") for project in task.projects if project.name):
        rsync_cmd = [nsc.RSYNC, '-rlt']
        rsync_cmd += ['--chmod=a+rX,ug+w']
        rsync_cmd += SAV_INCLUDE_PATHS
        rsync_cmd += [os.path.join(SAV_REPOSITORY_DIR, task.run_id)]
        rcode = remote.run_command(rsync_cmd, task, "rsync", time="01:00", storage_job=True, cwd=task.work_dir)
        # Note: Rsync error code is ignored. It will return an error if not all input files exist.

    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

