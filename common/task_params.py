import argparse
import utilities
import nsc

# TODO: rename to task.py


# Standard argument definitions
# (name, argparse_name, udf_name, type, default)
ARG_OPTIONS = {
        "src_dir": ("SRC-DIR", str, None, "Source directory (run folder)"),
        "work_dir": ("DIR", str, None, "Destination/working directory (run folder)"),
        "threads": ("--threads", int, 1, "Number of threads/cores to use"),
        }

class Task(object): 
    """Class to manage the processing tasks (scripts) in a common way for LIMS
    and non-LIMS invocation.

    Acts as a context manager (use with "with <object>") to detect errors and
    report them to the LIMS (replaces error_reporter class in v1).
    
    Implements a "proxy" for retrieving information which is done differently
    in LIMS and non-LIMS (lane stats).
    """

    def __init__(self, task_name, task_description):
        self.work_dir = None
        self.src_dir = None
        self.threads = None
        self.args = []
        self.parser = argparse.ArgumentParser(description=task_description)
        self.parser.add_argument("--pid", default=None, help="Process ID if running within LIMS")
        self.process = None
        self.finished = False
        

    def set_args(self, arg_list=['work-dir']):
        self.args = arg_list
        for name in self.args:
            argparse_name, udf_name, type, default, help = self.args[arg_name]
            self.parser.add_argument(
                    argparse_name,
                    destination=name,
                    type=type,
                    default=default,
                    help=help
                    )


    def start(self):
        """Start running the task. Will exit() if the args are
        incorrect."""

        self.parser.parse()
        if self.parser.pid:
            # LIMS operation
            from genologics.lims import *
            self.process = Process(nsc.lims, id=self.parser.pid)

        else:
            self.process = None

        self.process = None
        for arg in self.args:
            pass


    def finish(self):
        self.finished = True
    

    def get_arg(self, arg_name):
        argparse_name, udf_name, type, default, help = self.args[arg_name]
        if self.process:
            return utilities.get_udf(self.process, udf_name, default)
        else:
            return self.parser.getattr(arg_name)

    @property
    def work_dir(self):
        return self.get_arg('work_dir')

    @property
    def src_dir(self):
        return self.get_arg('src_dir')

    @property
    def threads(self):
        return self.get_arg('threads')


    # Context manager protocol: __enter__ and __exit__
    def __enter__(self):
        pass

    def __exit__(self, etype, value, tb):
        if etype is None:
            if self.finished:
                return True
            else:
                print "Uexpected exit"
                if self.process:
                    utilities.fail(self.process, "Unexpected exit", "Unexpected exit without exception")
                return False

        if not self.process_id:
            if len(sys.argv) == 2:
                self.process_id = sys.argv[1]

        if self.process:
            process = Process(nsc.lims, id=self.process_id)
            utilities.fail(self.process, etype.__name__ + " " + str(value),
                    "\n".join(traceback.format_exception(etype, value, tb)))

        return False # re-raise exception

    def running(process, current_job, status = None):
        process.get()
        if status:
            process.udf[nsc.JOB_STATUS_UDF] = "Running ({0})".format(status)
        else:
            process.udf[nsc.JOB_STATUS_UDF] = "Running"
        process.udf[nsc.JOB_STATE_CODE_UDF] = 'RUNNING'
        process.udf[nsc.CURRENT_JOB_UDF] = current_job
        process.put()


    def fail(process, message, extra_info = None):
        """Report failure from background job"""

        process.get(force=True)
        process.udf[nsc.JOB_STATUS_UDF] = "Failed: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + ": " + message
        process.udf[nsc.JOB_STATE_CODE_UDF] = 'FAILED'
        process.put()
        if extra_info:
            try:
                process.udf[nsc.ERROR_DETAILS_UDF] = extra_info
                process.put()
            except (KeyError,requests.exceptions.HTTPError):
                pass


    def success_finish(process):
        """Called by background jobs (slurm) to declare that the task has been 
        completed successfully."""

        process.get()
        process.udf[nsc.JOB_STATUS_UDF] = 'Completed successfully ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        process.udf[nsc.JOB_STATE_CODE_UDF] = 'COMPLETED'
        process.put()
