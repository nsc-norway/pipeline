#!/bin/env python

# Script to submit slurm jobs from EPP triggers
# This script calls slurm with the correct configuration for the site,
# then records the job-id in a UDF.


import sys, os.path
import argparse
import subprocess
from genologics.lims import *

import nsc
import utilities




def submit_job(script, script_args):
    logfile = os.path.join(nsc.LOG_DIR, 'slurm-%j.out')
    args = ['--output=' + logfile, '--error=' + logfile, '--parsable']
    args.append(script)
    args += script_args
    output = utilities.check_output([nsc.SBATCH] + args)
    return int(output)


def post_job_id(process_id, job_id_udf, job_id):
    print "Would set job id to ", job_id


def main(process_id, job_id_udf, script, args):
    job_id = submit_job(script, args)
    if process_id and job_id_udf:
        post_job_id(process_id, job_id_udf, job_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--pid', help='Process ID')
    parser.add_argument('--jid-udf', default="Job ID", help='Name of Job ID UDF')
    parser.add_argument('script', help='Script to submit to sbatch')
    parser.add_argument('job_args', nargs=argparse.REMAINDER, help='Arguments to pass to the job script')

    args = parser.parse_args()

    nsc.lims.check_version()
    main(args.pid, args.jid_udf, args.script, args.job_args)

