# Move run into processed/ on primary storage, and mark as completed
# in LIMS 

import sys
import os
import datetime
from genologics.lims import *
from common import nsc, taskmgr

TASK_NAME = '99. Mark "processed"'
TASK_DESCRIPTION = """Tasks to run when demultiplexing step is closed in LIMS."""
TASK_ARGS = ['src_dir', 'lanes']


def main(task):
    task.running()

    inputs = task.process.all_inputs(unique=True)
    if task.process:
        inputs = task.process.all_inputs(unique=True, resolve=True)
        lims_samples = task.lims.get_batch(set(sample for i in inputs for sample in i.samples))
        for lims_project in set(sample.project for sample in lims_samples):
            # Have to check for existence; controls don't have project
            if lims_project and lims_project.udf.get('Project type') in ['FHI-Covid19', 'MIK-Covid19']:
                lims_project.close_date = datetime.date.today()
                lims_project.put()
    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

