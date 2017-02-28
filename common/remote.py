# Slurm script

import os
import subprocess
import getpass
import tempfile
import StringIO
import itertools
import multiprocessing
import time

import nsc
import utilities

local_job_id = 1

def srun_command(
        args, task, jobname, jobtime, logfile=None,
        cpus_per_task=1, mem=1024, bandwidth=0, cwd=None,
        stdout=None, srun_user_args=[],
        storage_job=False, comment=None
        ):
    sbatch_other_args = [
            '--job-name=' + jobname,
            '--cpus-per-task=' + str(cpus_per_task),
            '--time=' + jobtime,
            '--mem=' + str(mem)
            ] + srun_user_args
    if bandwidth != 0:
        sbatch_other_args.append('--gres=rsc:' + str(bandwidth))
    elif storage_job:
        sbatch_other_args.append('--gres=rsc:1G')
    if logfile:
        logpath = os.path.realpath(logfile)
        sbatch_other_args += ['--output=' + logpath, '--error=' + logpath]
        if stdout:
            raise ValueError("Options stdout and logfile may not be used at the same time")
    elif stdout:
        stdoutpath = os.path.realpath(stdout)
        sbatch_other_args += ['--output=' + stdoutpath]

    if storage_job:
        sbatch_other_args += nsc.SRUN_STORAGE_JOB_ARGS

    if comment:
        sbatch_other_args += ['--comment=' + comment]

    arglist = nsc.SBATCH_ARGLIST + ['--parsable']
    cmd = "'" + "' '".join(arg.replace("'", "'\\''") for arg in args) + "'"

    job_id = utilities.check_output(arglist + sbatch_other_args + ['--wrap', cmd] , cwd=cwd)
    complete = False
    delay = 2
    while not complete:
        try:
            state = utilities.check_output(nsc.SQUEUE + ['-j', job_id, '-O', 'State', '-h', '-t', 'all']).strip()
        except subprocess.CalledProcessError:
            state = "UNKNOWN"
        complete = state in ['FAILED', 'CANCELLED', 'COMPLETED']
        if task:
            task.info(jobname + " " + state.lower())
        if not complete:
            time.sleep(delay)
            delay = 30
    if state == "COMPLETED":
        return 0
    else:
        return 1


def local_command(args, task, logfile=None, cwd=None, stdout=None):
    if not cwd:
        cwd = os.getcwd()
    if logfile:
        stdoutfile = open(logfile, "w")
        stderrfile = stdoutfile
    elif stdout:
        stdoutfile = open(stdout, "w")
        stderrfile = None
    else:
        stdoutfile = None
        stderrfile = None

    return subprocess.call(args, stdout=stdoutfile, stderr=stderrfile, cwd=cwd)


def run_command(
        args, task, jobname, time, logfile=None,
        cpus=1, mem=1024, bandwidth=0, cwd=None,
        stdout=None, srun_user_args=[],
        storage_job=False, comment=None
        ):
    if nsc.REMOTE_MODE == "srun":
        return srun_command(
            args, task, jobname, time, logfile, cpus, mem, bandwidth, cwd,
            stdout, srun_user_args, storage_job, comment
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

class SlurmArrayJob(object):
    def __init__(self, arg_lists, jobname, time, stdout_pattern):
        
        self.arg_lists = arg_lists
        self.jobname = jobname
        self.time = time
        self.stdout_pattern = stdout_pattern
        self.cpus_per_task = 1
        self.max_simultaneous = None
        self.mem_per_task = 1024
        self.bandwidth_per_task = 0
        self.cwd = None
        self.comment = None

        self.job_id = None
        self.states = {}
        self.summary = {}

    def start(self):
        handle, path = tempfile.mkstemp()
        os.write(handle, "#!/bin/bash\n\n")
        os.write(handle, "#SBATCH --job-name=\"{0}\"\n".format(self.jobname))
        os.write(handle, "#SBATCH --time={0}\n".format(self.time))
        os.write(handle, "#SBATCH --output=\"{0}\"\n".format(self.stdout_pattern))
        if self.cpus_per_task:
            os.write(handle, "#SBATCH --cpus-per-task={0}\n".format(self.cpus_per_task))
        if self.mem_per_task:
            os.write(handle, "#SBATCH --mem={0}\n".format(self.mem_per_task))
        if self.bandwidth_per_task != 0:
            os.write(handle, "#SBATCH --gres=rsc:{0}\n".format(self.bandwidth_per_task))
        if self.comment:
            os.write(handle, "#SBATCH --comment=\"{0}\"\n".format(self.comment))


        for i, arg_list in enumerate(self.arg_lists):
            argv = " ".join("'" + s.replace("'", "'\\''") + "'" for s in arg_list)
            os.write(handle, "[ $SLURM_ARRAY_TASK_ID == {0} ] && {1} && exit 0\n".format(i, argv))
        os.write(handle, "exit 1\n")
        os.close(handle)
        
        array = '--array=0-'+str(len(self.arg_lists) - 1)
        if self.max_simultaneous is not None:
            array += "%%%d" % (self.max_simultaneous)
        self.job_id = utilities.check_output(nsc.SBATCH_ARGLIST + ['--parsable', array, path], cwd=self.cwd).strip()
        self.states = dict((str(j), 'PENDING') for j in range(len(self.arg_lists)))
        self.summary = {'PENDING': len(self.arg_lists)}
        os.remove(path)

    def check_status(self):
        """Refresh status of jobs. Should be called periodically (every minute)."""
        try:
            squeue_out = utilities.check_output(nsc.SQUEUE + ['-j', self.job_id, '-O', 'ArrayTaskID,State', '-h', '-t', 'all', '-r'])
        except subprocess.CalledProcessError:
            squeue_out = ""
        new_states = dict(line.split() for line in squeue_out.splitlines() if line)
    
        # If this job cancelled, make sure others are marked as the same state too.
        if new_states and all(state in set(('COMPLETED', 'FAILED', 'CANCELLED')) for state in new_states.values()):
            for jix in self.states.keys():
                if not jix in new_states.keys() and self.states[jix] == "PENDING":
                    self.states[jix] = 'CANCELLED'

        self.states.update(new_states)
        if not squeue_out and "RUNNING" in self.states.values():
            raise JobMonitoringException()
        self.summary = dict((key, len(list(group))) for key, group in itertools.groupby(sorted(self.states.values())))

    @property
    def is_finished(self):
        return all(state in set(('COMPLETED', 'FAILED', 'CANCELLED')) for state in self.states.values())

    @staticmethod
    def start_jobs(jobs, max_local_threads):
        for job in jobs:
            job.start()

    @staticmethod
    def update_status(jobs):
        for job in jobs:
            job.check_status()


def local_execute(arg_list, logfile, cwd):
    res = local_command(arg_list, logfile, cwd)
    if res != 0:
        raise subprocess.CalledProcessError("Non-zero exit code")


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
        self.bandwidth_per_task = 0
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


if nsc.REMOTE_MODE == "srun":
    ArrayJob = SlurmArrayJob
elif nsc.REMOTE_MODE == "local":
    ArrayJob = LocalArrayJob


