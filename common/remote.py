# Slurm script

import os
import subprocess
import getpass
import tempfile
import io
import itertools
import multiprocessing
import time

from . import nsc
from . import utilities

local_job_id = 1

def srun_command(
        args, task, jobname, jobtime, logfile,
        cpus_per_task=1, mem=1024, cwd=None, stdout=None, 
        srun_user_args=[], comment=None
        ):
    sbatch_other_args = [
            '--job-name=' + jobname,
            '--cpus-per-task=' + str(cpus_per_task),
            '--time=' + jobtime,
            '--mem=' + str(mem)
            ] + srun_user_args
    logpath = os.path.realpath(logfile)
    if stdout:
        stdoutpath = os.path.realpath(stdout)
        sbatch_other_args += ['--output=' + stdoutpath, '--error=' + logpath]
    else:
        sbatch_other_args += ['--output=' + logpath, '--error=' + logpath]

    if comment:
        sbatch_other_args += ['--comment=' + comment]

    arglist = nsc.SBATCH_ARGLIST + ['--parsable']
    cmd = "'" + "' '".join(arg.replace("'", "'\\''") for arg in args) + "'"

    job_id = utilities.check_output(arglist + sbatch_other_args + ['--wrap', cmd] , cwd=cwd).strip()
    complete = False
    delay = 2
    while not complete:
        time.sleep(delay)
        delay = min(30, delay + 1)
        try:
            data = utilities.check_output(nsc.SQUEUE + ['-j', job_id, '-O', 'State,NodeList', '-h', '-t', 'all'])
            parts = data.split()
            if len(parts) == 2:
                state, node = parts
            else:
                state, node = (parts[0], None)
        except (subprocess.CalledProcessError, ValueError):
            state = "UNKNOWN"
        complete = state in ['FAILED', 'CANCELLED', 'COMPLETED', 'TIMEOUT']
        if task:
            task.job_status(job_id, jobname, state.lower(), node)

    if state == "COMPLETED":
        return 0
    else:
        return 1


def local_command(args, logfile, cwd=None, stdout=None):
    if not cwd:
        cwd = os.getcwd()
    stderrfile = open(logfile, "w")
    if stdout:
        stdoutfile = open(stdout, "w")
    else:
        stdoutfile = stderrfile
    return subprocess.call(args, stdout=stdoutfile, stderr=stderrfile, cwd=cwd)


def run_command(
        args, task, jobname, time, logfile=None,
        cpus=1, mem=1024, cwd=None, stdout=None,
        srun_user_args=[], comment=None
        ):
    if not logfile:
        logfile = task.logfile(jobname)
    if nsc.REMOTE_MODE == "srun":
        return srun_command(
            args, task, jobname, time, logfile, cpus, mem, cwd,
            stdout, srun_user_args, comment
            )
    elif nsc.REMOTE_MODE == "local": 
        return local_command(args, logfile, cwd, stdout)

class JobMonitoringException(Exception):
    pass


# ArrayJob architecture excuse:
# There are different implementations of ArrayJob depending on the 
# execution backend: local or slurm.
# The relevant class for the current environment is then aliased to ArrayJob
# depending on the configuration. So far so good?
# Well, sometimes one needs to start and montior multiple job 
# arrays with different parameters. For local jobs, it should be coordinated 
# so that the total number of concurrent jobs is limited (not just the jobs
# for each array separately).

# To allow coordination between different job arrays, the starting and polling 
# of jobs is handled by static methods on the ArrayJob class: start_jobs(),
# update_status(). Thus, instances of different implementations cannt be used 
# together (use the ArrayJob alias, not SlurmArrayJob, LocalArrayJob).

# Do use the static methods start_jobs() and update_status(), and not _start and
# _check_status. The latter methods are not implemented for LocalArrayJob.

class SlurmArrayJob(object):
    def __init__(self, arg_lists, jobname, time, stdout_pattern):
        
        self.arg_lists = arg_lists
        self.jobname = jobname
        self.time = time
        self.stdout_pattern = stdout_pattern
        self.cpus_per_task = 1
        self.max_simultaneous = None
        self.mem_per_task = 1024
        self.cwd = None
        self.comment = None

        self.job_id = None
        self.states = {}
        self.summary = {}

    def _start(self):
        handle, path = tempfile.mkstemp(text=True)
        with tempfile.NamedTemporaryFile(mode='w') as outputfile:
            outputfile.write("#!/bin/bash\n\n")
            outputfile.write("#SBATCH --job-name=\"{0}\"\n".format(self.jobname))
            outputfile.write("#SBATCH --time={0}\n".format(self.time))
            outputfile.write("#SBATCH --output=\"{0}\"\n".format(self.stdout_pattern))
            if self.cpus_per_task:
                outputfile.write("#SBATCH --cpus-per-task={0}\n".format(self.cpus_per_task))
            if self.mem_per_task:
                outputfile.write("#SBATCH --mem={0}\n".format(self.mem_per_task))
            if self.comment:
                outputfile.write("#SBATCH --comment=\"{0}\"\n".format(self.comment))


            for i, arg_list in enumerate(self.arg_lists):
                argv = " ".join("'" + s.replace("'", "'\\''") + "'" for s in arg_list)
                outputfile.write("[ $SLURM_ARRAY_TASK_ID == {0} ] && {1} && exit 0\n".format(i, argv))
            outputfile.write("exit 1\n")
            outputfile.flush()

            if not self.arg_lists:
                raise ValueError("Starting SLURM array job {}: The list of jobs is empty (check sample sheet).".format(self.jobname))
            array = '--array=0-'+str(len(self.arg_lists) - 1)
            if self.max_simultaneous is not None:
                array += "%%%d" % (self.max_simultaneous)
            self.job_id = utilities.check_output(nsc.SBATCH_ARGLIST + ['--parsable', array, outputfile.name], cwd=self.cwd).strip()
            self.states = dict((str(j), 'PENDING') for j in range(len(self.arg_lists)))
            self.summary = {'PENDING': len(self.arg_lists)}

    def _check_status(self):
        """Refresh status of jobs. Should be called periodically (every minute)."""
        try:
            squeue_out = utilities.check_output(nsc.SQUEUE + ['-j', self.job_id, '-O', 'ArrayTaskID,State', '-h', '-t', 'all', '-r'])
        except subprocess.CalledProcessError:
            squeue_out = ""
        new_states = dict(line.split() for line in squeue_out.splitlines() if line)
    
        # If this job cancelled, make sure others are marked as the same state too.
        if new_states and all(state in set(('COMPLETED', 'FAILED', 'CANCELLED')) for state in list(new_states.values())):
            for jix in list(self.states.keys()):
                if not jix in list(new_states.keys()) and self.states[jix] == "PENDING":
                    self.states[jix] = 'CANCELLED'

        self.states.update(new_states)
        #if not squeue_out and "RUNNING" in self.states.values():
        #    raise JobMonitoringException()
        self.summary = dict((key, len(list(group))) for key, group in itertools.groupby(sorted(self.states.values())))

    @property
    def is_finished(self):
        return all(state in set(('COMPLETED', 'FAILED', 'CANCELLED')) for state in list(self.states.values()))

    @staticmethod
    def start_jobs(jobs, max_local_threads):
        for job in jobs:
            job._start()

    @staticmethod
    def update_status(jobs):
        for job in jobs:
            job._check_status()


def local_execute(arg_list, logfile, cwd):
    res = local_command(arg_list, logfile, cwd)
    if res != 0:
        raise subprocess.CalledProcessError(res, str(arg_list))


class LocalArrayJob(object):
    """Local process pool.
    
    It only supports jobs which require a single CPU (this is a lot easier
    to implement, since it works with the multiprocessing package)."""
    def __init__(self, arg_lists, jobname, time, stdout_pattern):
        global local_job_id
        self.job_id = local_job_id
        local_job_id += 1
        self.arg_lists = arg_lists
        self.stdout_pattern = stdout_pattern
        self.summary = {"PENDING": len(arg_lists)}
        self.cwd = None
        self.results = []
        self.pool = None
        self.max_async = 0
        self.is_finished = False
        self.mem_per_task = 1024
        self.cpus_per_task = 1
        self.jobname = jobname
        self.comment = None

    @staticmethod
    def start_jobs(jobs, max_local_threads=None):
        pool = multiprocessing.Pool(max_local_threads)
        for job in jobs:
            job.results = []
            job.pool = pool
            job.max_async = max_local_threads
            for i, arg_list in enumerate(job.arg_lists):
                logfile = job.stdout_pattern.replace("%a", str(i))
                res = pool.apply_async(local_execute, [arg_list, logfile, job.cwd])
                job.results.append(res)

    @staticmethod
    def update_status(jobs):
        total_running = 0
        for job in jobs:
            completed, failed, running, pending = 0,0,0,0
            for i, res in enumerate(job.results):
                if res.ready():
                    if res.successful():
                        completed += 1
                    else:
                        failed += 1
                else:
                    if total_running < job.max_async:
                        running += 1
                        total_running += 1
                    else:
                        pending += 1
            # Only add to summary if non-zero
            job.summary = dict(
                    [(k, v) for k, v in
                    [("COMPLETED", completed), ("FAILED", failed),
                     ("RUNNING", running), ("PENDING", pending)]
                    if v > 0
                    ])
            job.is_finished = running == 0 and pending == 0


class SerialArrayJob(object):
    """Run each job one by one. Used for testing."""
    def __init__(self, arg_lists, jobname, time, stdout_pattern):
        global local_job_id
        self.job_id = local_job_id
        local_job_id += 1
        self.arg_lists = arg_lists
        self.stdout_pattern = stdout_pattern
        self.summary = {"PENDING": len(arg_lists)}
        self.cwd = None
        self.results = []
        self.is_finished = False
        self.mem_per_task = 1024
        self.cpus_per_task = 1
        self.jobname = jobname
        self.comment = None

    @staticmethod
    def start_jobs(jobs, max_local_threads=None):
        for job in jobs:
            job.results = []
            for i, arg_list in enumerate(job.arg_lists):
                logfile = job.stdout_pattern.replace("%a", str(i))
                job.summary = {}
                try:
                    local_execute(arg_list, logfile, job.cwd)
                    job.summary['COMPLETED'] = job.summary.get('COMPLETED', 0) + len(arg_list)
                except subprocess.CalledProcessError:
                    job.summary['FAILED'] = job.summary.get('FAILED', 0) + len(arg_list)
            job.is_finished = True

    @staticmethod
    def update_status(jobs):
        pass


if nsc.REMOTE_MODE == "srun":
    ArrayJob = SlurmArrayJob
elif nsc.REMOTE_MODE == "local":
    ArrayJob = LocalArrayJob


