#!/bin/env python

# Script to submit slurm jobs from EPP triggers
# This script calls slurm with the correct configuration for the site,
# then records the job-id in a UDF.

# The script must be installed in the customer extensions directory, 
# because it needs to be readable by the glsai user.


import sys, os.path
import argparse
import subprocess
from genologics.lims import *

from common import nsc, utilities


def submit_job(memory, nthreads, jobname, script, script_args):
    logfile = os.path.join(nsc.LOG_DIR, 'slurm-%j.out')
    args = ['--output=' + logfile, '--error=' + logfile, '--parsable',
            '--job-name=' + jobname, '--cpus-per-task=' + str(nthreads),
            '--mem=' + str(memory)]
    args.append(script)
    args += script_args
    output = utilities.check_output(nsc.INVOKE_SBATCH_ARGLIST + args)
    return int(output)


def post_job_id(process, job_id):
    process.udf[nsc.JOB_ID_UDF] = job_id
    process.udf[nsc.JOB_STATUS_UDF] = 'Submitted'
    process.put()


def main(process_id, memory, threadmem, nthreads, inputthreads, jobname, script, args):
    process = Process(nsc.lims, id=process_id)

    if inputthreads:
        nthreads += len(process.all_inputs(unique=True))*inputthreads
    if threadmem: 
        memory += nthreads*threadmem

    job_id = submit_job(memory, nthreads, jobname, script, args)
    post_job_id(process, job_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--pid', help='Process ID')
    parser.add_argument('--threads', type=int, help='Number of threads', default=1)
    parser.add_argument('--inputthreads', type=int, help='Number of (extra) threads to request per process input')
    parser.add_argument('--mem', type=int, help='Memory to request for the process (megabytes)', default=1024)
    parser.add_argument('--thread-mem', type=int, help='Additional memory to request per thread')
    parser.add_argument('--jobname', help='Job name')
    parser.add_argument('script', help='Script to submit to sbatch')
    parser.add_argument('job_args', nargs=argparse.REMAINDER, help='Arguments to pass to the job script')

    args = parser.parse_args()

    nsc.lims.check_version()
    main(args.pid, args.mem, args.thread_mem, args.threads, args.inputthreads, args.jobname, args.script, args.job_args)

