#!/bin/env python

# Crashed job detector (to be run in cron job, etc)
# Checks slurm status for jobs with RUNNING or SUBMITTED
# status in LIMS.


import sys
import os.path
import re
import datetime
import subprocess
from genologics.lims import *

from common import nsc, utilities


def get_active_jobs():
    types = [
            step.name
            for wf, steps in nsc.AUTOMATED_PROTOCOL_STEPS
            for step in steps
            ]
    running, submitted = [], []
    for type in types:
        running += nsc.lims.get_processes(type=type, udf={'Job state code': 'RUNNING'})
        submitted += nsc.lims.get_processes(type=type, udf={'Job state code': 'SUBMITTED'})
    return running, submitted


def check_job(process):
    if process.udf[nsc.JOB_STATE_CODE_UDF] in ('RUNNING', 'SUBMITTED'):
        args = nsc.SCONTROL_STATUS_ARGLIST + [str(process.udf[nsc.JOB_ID_UDF])]
        failed = False
        try:
            info = utilities.check_output(args)
            state_match = re.search(r"\bJobState=([A-Z]+)\b", info)
            failed = state_match.group(1) not in ('RUNNING', 'PENDING')
        except subprocess.CalledProcessError:
            failed = True

        if failed:
            process.udf[nsc.JOB_STATE_CODE_UDF] = "FAILED"
            process.udf[nsc.JOB_STATUS_UDF] = "Failed: Crashed job detector " +\
                    str(datetime.date.today())
            process.put()



def main():
    running, submitted = get_active_jobs()
    for job in running + submitted:
        check_job(job)


if __name__ == "__main__":
    main()

