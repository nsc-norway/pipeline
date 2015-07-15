# Slurm script

def srun_command(
        args, jobname, logfile, time,
        cpus_per_task=1, mem=1024
        ):

    other_args = [
            '--output=' + logfile, '--error=' + logfile,
            '--job-name=' + jobname,
            '--cpus-per-task=' + str(cpus_per_task),
            '--time=' + time,
            '--mem=' + str(mem)
            ]

    subprocess.check_call(nsc.SRUN_ARGLIST + args + other_args)

