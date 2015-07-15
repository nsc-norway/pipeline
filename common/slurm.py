# Slurm script

def srun_command(
        args, jobname, time, logfile="/dev/null",
        cpus_per_task=1, mem=1024,
        srun_args=[]
        ):
    other_args = [
            '--job-name=' + jobname,
            '--cpus-per-task=' + str(cpus_per_task),
            '--time=' + time,
            '--mem=' + str(mem)
            ]
    other_args += ['--output=' + logfile, '--error=' + logfile]
    return subprocess.call(nsc.SRUN_ARGLIST + args + other_args)

