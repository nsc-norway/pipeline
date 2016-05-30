# Slurm script

import os
import subprocess
import getpass
import tempfile
import StringIO
from itertools import groupby

import nsc
import utilities

def srun_command(
        args, jobname, time, logfile=None,
        cpus_per_task=1, mem=1024, cwd=None,
        stdout=None, srun_user_args=[],
        change_user=True, storage_job=False,
        comment=None
        ):
    srun_other_args = [
            '--job-name=' + jobname,
            '--cpus-per-task=' + str(cpus_per_task),
            '--time=' + time,
            '--mem=' + str(mem)
            ] + srun_user_args
    if logfile:
        logpath = os.path.realpath(logfile)
        srun_other_args += ['--output=' + logpath, '--error=' + logpath]
        if stdout:
            raise ValueError("Options stdout and logfile may not be used at the same time")
    elif stdout:
        stdoutpath = os.path.realpath(stdout)
        srun_other_args += ['--output=' + stdoutpath]

    if storage_job:
        srun_other_args += nsc.SRUN_STORAGE_JOB_ARGS

    if comment:
        srun_other_args += ['--comment=' + comment]

    if change_user:
        arglist = nsc.SRUN_GLSAI_ARGLIST
        if not cwd:
            # sort of a heuristic, when in change_user mode it's typically 
            # run by LIMS, and then it will start in a dir which only exists
            # on the LIMS server -- so instead we move to /tmp
            cwd = "/tmp"
    else:
        arglist = nsc.SRUN_OTHER_ARGLIST

    return subprocess.call(arglist + srun_other_args + args , cwd=cwd)


def local_command(args, logfile=None, cwd=None, stdout=None):
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
        args, jobname, time, logfile=None,
        cpus=1, mem=1024, cwd=None,
        stdout=None, srun_user_args=[],
        change_user=True, storage_job=False,
        comment=None
        ):
    if nsc.REMOTE_MODE == "srun":
        change_user = getpass.getuser() == "glsai" 
        return srun_command(
            args, jobname, time, logfile, cpus, mem, cwd,
            stdout, srun_user_args, change_user, storage_job,
            comment
            )
    elif nsc.REMOTE_MODE == "local": 
        return local_command(args, logfile, cwd, stdout)

class JobMonitoringException(Exception):
    pass

class ArrayJob(object):

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
        self.states = dict((str(j), 'PENDING') for j in range(int(self.job_id), int(self.job_id)+len(self.arg_lists)))
        self.summary = {'PENDING': len(self.arg_lists)}
        os.remove(path)

    def check_status(self):
        """Refresh status of jobs. Should be called at least every 15 seconds to avoid missing jobs."""
        try:
            squeue_out = utilities.check_output(nsc.SQUEUE + ['-j', self.job_id, '-O', 'JobID,State', '-h', '-t', 'all', '-r'])
        except subprocess.CalledProcessError:
            squeue_out = ""
        new_states = dict(line.split() for line in squeue_out.splitlines() if line)
    
        # State of subjob with same ID as array job will be PENDING until no more jobs are pending.
        # If this job cancelled, make sure others are marked as the same state too.
        if new_states and all(state in set(('COMPLETED', 'FAILED', 'CANCELLED')) for state in new_states.values()):
            for jid in self.states.keys():
                if not jid in new_states.keys() and self.states[jid] == "PENDING":
                    self.states[jid] = 'CANCELLED'

        self.states.update(new_states)
        if not squeue_out and "RUNNING" in self.states.values():
            raise JobMonitoringException()
        self.summary = dict((key, len(list(group))) for key, group in groupby(sorted(self.states.values())))

    @property
    def is_finished(self):
        return all(state in set(('COMPLETED', 'FAILED', 'CANCELLED')) for state in self.states.values())

def is_scheduler_available():
    return nsc.REMOTE_MODE == "srun"

