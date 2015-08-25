# Slurm script

import subprocess
import nsc
import os

def srun_command(
        args, jobname, time, logfile=None,
        cpus_per_task=1, mem=1024, cwd=None,
        stdout=None, srun_user_args=[]
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

    if not cwd:
        cwd = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    return subprocess.call(nsc.SRUN_ARGLIST + srun_other_args + args , cwd=cwd)

