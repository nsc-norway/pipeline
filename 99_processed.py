# Move run into processed/ on primary storage, and mark as completed
# in LIMS 

import sys
import os
from datetime import date
from genologics.lims import *
from common import nsc, taskmgr

TASK_NAME = '99. Mark "processed"'
TASK_DESCRIPTION = """Move original run folder into processed/ directory
                    on primary storage, since we are done with it."""
TASK_ARGS = ['src_dir']


def main(task):
    task.running()

    inputs = task.process.all_inputs(unique=True)

    if not task.process or all(input.qc_flag == "PASSED" for input in inputs):
        if os.path.exists(
                os.path.join(nsc.PRIMARY_STORAGE, "processed", task.run_id)
                ):
            task.info("Run " + task.run_id + " is already in processed directory")
        else:
            task.info("Moving " + task.src_dir + " to processed directory")
            os.rename(
                    task.src_dir,
                    os.path.join(nsc.PRIMARY_STORAGE, "processed", task.run_id)
                    )
    else:
        task.info("Not moving to processed because some samples failed")

    task.success_finish()


with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
    main(task)

