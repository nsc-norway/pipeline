# Delay script
# This script waits for an hour and then returns successfully

import time
import datetime
from common import taskmgr

TASK_NAME = "10. Delay"
TASK_DESCRIPTION = "Wait for 1 hour"
TASK_ARGS = ['work_dir']

def main(task):
    task.running()
    task.info("Waiting for an hour (started waiting at {})...".format(
                datetime.datetime.now()
                ))
    time.sleep(3600)
    task.success_finish()

if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

