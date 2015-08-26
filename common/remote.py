# Slurm script

import subprocess
import nsc
import os

def srun_command(
        args, jobname, time, logfile=None,
        cpus_per_task=1, mem=1024, cwd=None,
        stdout=None, srun_user_args=[],
        change_user=True
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

    if change_user:
        arglist = nsc.SRUN_GLSAI_ARGLIST
    else:
        arglist = nsc.SRUN_OTHER_ARGLIST

    return subprocess.call(arglist + srun_other_args + args , cwd=cwd)


def run_command(
        args, jobname, time, logfile=None,
        cpus_per_task=1, mem=1024, cwd=None,
        stdout=None, srun_user_args=[],
        change_user=True
        ):
    return srun_command(
        args, jobname, time, logfile, cpus_per_task, mem, cwd,
        stdout, srun_user_args, change_user
        )

