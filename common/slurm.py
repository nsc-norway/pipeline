# Slurm script

import subprocess
import nsc

def srun_command(
        args, jobname, time, logfile=None,
        cpus_per_task=1, mem=1024, cwd=None,
        srun_args=[]
        ):
    srun_other_args = [
            '--job-name=' + jobname,
            '--cpus-per-task=' + str(cpus_per_task),
            '--time=' + time,
            '--mem=' + str(mem)
            ]
    if logfile:
        srun_other_args += ['--output=' + logfile, '--error=' + logfile]
    return subprocess.call(nsc.SRUN_ARGLIST + srun_other_args + args , cwd=cwd)

