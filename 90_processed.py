# Move run into processed/ on primary storage, and mark as completed
# in LIMS 

import sys
import os
from datetime import date
from genologics.lims import *
from common import nsc, taskmgr

TASK_NAME = '90. Move to "processed"'
TASK_DESCRIPTION = """Do some bookkeeping and move original run folder into processed/ 
                    directory on primary storage, since we are done with it."""
TASK_ARGS = ['src_dir']


def main(task):
    task.running()

    runid = task.run_id

    if task.process:
        inputs = task.process.all_inputs(unique=True)
        flowcells = set(i.location[0] for i in inputs)
        if len(flowcells) == 1:
            fc = next(iter(flowcells))
            fc.get()
            # Tracking UDF for "overview" page
            fc.udf[nsc.RECENTLY_COMPLETED_UDF] = True
            fc.udf[nsc.PROCESSED_DATE_UDF] = date.today()
            fc.put()


    print "Moving", task.src_dir, "to processed directory"
    os.rename(
            task.src_dir,
            os.path.join(nsc.PRIMARY_STORAGE, "processed", task.run_id)
            )

    task.success_finish()


with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
    main(task)

