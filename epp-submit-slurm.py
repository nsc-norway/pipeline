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


def submit_job(memory, nthreads, jobname, script, script_args):
    logfile = os.path.join(nsc.LOG_DIR, 'slurm-%j.out')
    args = ['--output=' + logfile, '--error=' + logfile, '--parsable',
            '--job-name=' + jobname, '--nthreads=' + nthreads, '--mem=' + memory]
    args.append(script)
    args += script_args
    output = utilities.check_output(nsc.INVOKE_SBATCH_ARGLIST + args)
    return int(output)


def post_job_id(process, job_id):
    process.udf[nsc.JOB_ID_UDF] = job_id
    process.udf[nsc.JOB_STATUS_UDF] = 'Submitted'
    process.put()


def main(process_id, memory, inputmem, nthreads, inputthreads, jobname, script, args):
    process = Process(nsc.lims, id=process_id)

    if inputthreads:
        nthreads += len(process.get_inputs(unique=True))*inputthreads
    if inputmem: 
        memory += len(process.get_inputs(unique=True))*inputmem

    job_id = submit_job(memory, nthreads, jobname, script, args)
    if process_id:
        post_job_id(process_id, job_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--pid', help='Process ID')
    parser.add_argument('--threads', help='Number of threads', default=1)
    parser.add_argument('--inputthreads', help='Number of (extra) threads to request per process input')
    parser.add_argument('--mem', help='Memory to request for the process (megabytes)', default=1024)
    parser.add_argument('--inputmem', help='Memory to request per process input (megabytes)')
    parser.add_argument('--jobname', help='Job name')
    parser.add_argument('script', help='Script to submit to sbatch')
    parser.add_argument('job_args', nargs=argparse.REMAINDER, help='Arguments to pass to the job script')

    args = parser.parse_args()

    nsc.lims.check_version()
    main(args.pid, args.script, args.mem, args.inputmem, args.nthreads, args.inputthreads, args.jobname, args.job_args)

