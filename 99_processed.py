# Move run into processed/ on primary storage, and mark as completed
# in LIMS 

import sys
import os
from datetime import datetime
from genologics.lims import *
from common import nsc, taskmgr

TASK_NAME = '99. Mark "processed"'
TASK_DESCRIPTION = """Tasks to run when demultiplexing step is closed in LIMS."""
TASK_ARGS = ['src_dir', 'lanes']


def main(task):
    task.running()

    inputs = task.process.all_inputs(unique=True)

    if os.path.exists(
            os.path.join(nsc.PRIMARY_STORAGE, "processed", task.run_id)
            ):
        task.info("Run " + task.run_id + " is already in processed directory")
    else:
        task.info("Will not move " + task.src_dir + " to processed directory, this has been disabled")
        #os.rename(
        #        task.src_dir,
        #        os.path.join(nsc.PRIMARY_STORAGE, "processed", task.run_id)
        #        )
    if task.process:
        inputs = task.process.all_inputs(unique=True, resolve=True)
        lims_samples = task.lims.get_batch(set(sample for i in inputs for sample in i.samples))
        for lims_project in set(sample.project for sample in lims_samples):
            if lims_project.udf.get('Project type') in ['FHI-Covid19', 'MIK-Covid19']:
                lims_project.closedate = datetime.now()
                lims_project.put()
    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

