# Slurm script

import subprocess
import nsc
import os
import getpass
import tempfile

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


class JobArray(object):
    
    def __init__(self, args_list, jobname, time, logfile=None,
        cpus_per_task=1, mem_per_task=1024, cwd=None, stdout=None,
        comment=None
        ):

        self.args_list = args_list
        self.jobname = jobname
        self.time = time
        self.logfile = logfile 
        self.cpus_per_task = cpus_per_task
        self.mem_per_task = mem_per_task
        self.cwd = cwd
        self.stdout = stdout
        self.comment = comment


    def start(self):
        change_user = getpass.getuser() == "glsai" 




    MAX_LINE = 256
    MAX_SIZE = 60000

    i_line = 0
    batch_i = 0
    index_commands = list(enumerate(args_list))
    while batch_i < len(index_commands):
        current_batch = []
        next_size = 0
        for i, c in index_commands[batch_i:]:
            i_line += 1
            format_line = "%d %s" % (i, " ".join(c))
            line_size = 1+len(format_line) # Includes line break
            if line_size >= MAX_LINE:
                raise ValueError("Line {0} is longer than {1} bytes".format(i_line, MAX_LINE))
            if next_size + line_size < MAX_SIZE:
                current_batch.append(c)
                next_size += line_size
            else:
                break
        else:
            i += 1 # If not break, we are done!

        batch_i = i

        commandfile_handle, path = tempfile.mkstemp()
        cmd_list = ["%d %s" % (i, " ".join(args)) for i, args in enumerate(current_batch)]
        os.write(commandfile_handle, "\n".join(cmd_list) + "\n")
        os.close(commandfile_handle)
        try:
            return srun_command([path], jobname, time, logfile,
                cpus_per_task, mem_per_task, cwd, stdout, 
                srun_user_args=['--multi-prog', '-l'],
                change_user=change_user, storage_job=False, comment=comment
                )
        finally:
            os.remove(path)


def is_scheduler_available():
    return nsc.REMOTE_MODE == "srun"

