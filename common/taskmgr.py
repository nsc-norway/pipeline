import argparse
import utilities
import nsc

from genologics.lims import *
# TODO: rename to task.py


# Module to manage processing status for all the script, and to abstract the 
# differences between command line invocation and invocation through LIMS.


# Standard argument definitions
# (name, argparse_name, udf_name, type, default)
ARG_OPTIONS = {
        "src_dir": ("SRC-DIR", str, None, "Source directory (run folder)"),
        "work_dir": ("DIR", str, None, "Destination/working directory (run folder)"),
        "threads": ("--threads", int, 1, "Number of threads/cores to use"),
        }
DEFAULT_VAL_INDEX = 2

class Task(object): 
    """Class to manage the processing tasks (scripts) in a common way for LIMS
    and non-LIMS invocation.

    Acts as a context manager (use with "with <object>") to detect errors and
    report them to the LIMS (replaces error_reporter class in v1).
    
    Implements a "proxy" for retrieving information which is done differently
    in LIMS and non-LIMS (lane stats).

    (Maintenance note: this constructor should not do anything that could fail,
    as it won't be reported to the LIMS)
    """

    def __init__(self, task_name, task_description, args):
        self.task_name = task_name
        self.task_description = task_description
        self.args = []
        self.parser = argparse.ArgumentParser(description=task_description)
        self.parser.add_argument("--pid", default=None, help="Process ID if running within LIMS")
        self.process = None
        self.finished = False


    def get_arg(self, arg_name):
        argparse_name, udf_name, type, default, help = self.args[arg_name]
        if self.process:
            return utilities.get_udf(self.process, udf_name, default)
        else:
            val = self.parser.getattr(arg_name)
            if val is None: # Handle when default gets updated
                return default

    @property
    def run_id(self):
        return self.get_arg("run_id")

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
        """Catch unexpected exceptions and also throw an error if exiting without
        success_finish. Called when exiting the with ... section.

        Note: many exit points in this function.
        """
        if etype is None:
            if self.finished:
                return True
            else:
                print "Uexpected exit"
                if self.process:
                    utilities.fail(self.process, "Unexpected exit", "Unexpected exit without exception")
                raise RuntimeError("Unexpected exit!")

        if self.process:
            process = Process(nsc.lims, id=self.process_id)
            utilities.fail(self.process, etype.__name__ + " " + str(value),
                    "\n".join(traceback.format_exception(etype, value, tb)))

        return False # re-raise exception



    # To be called to indicate the status
    def running(self, info_str=None):
        """Users should call this function once at the start of the task to 
        indicate that the program has started.

        parse_args() will exit() if the args are incorrect."""

        for name in self.args:
            argparse_name, udf_name, type, default, help = ARG_OPTIONS[name]
            self.parser.add_argument(
                    argparse_name,
                    destination=name,
                    type=type,
                    default=default,
                    help=help
                    )

        self.parser.parse()
        if self.parser.pid:
            # LIMS operation
            self.process = Process(nsc.lims, id=self.parser.pid)
            self.process.get()
            self.process.udf[nsc.JOB_STATUS_UDF] = "Running"
            self.process.udf[nsc.JOB_STATE_CODE_UDF] = 'RUNNING'
            self.process.udf[nsc.CURRENT_JOB_UDF] = self.task_name
            self.process.put()

            # Set defaults for source & working directories based on run ID
            # (only available for LIMS)
            try:
                run_id = self.process.udf[nsc.RUN_ID_UDF]
                work_dir = os.path.join(nsc.PRIMARY_STORAGE, run_id)
                # These defaults are set to None in the ARG_OPTIONS initialization,
                # no need to check if they are None
                ARG_OPTIONS['src_dir'][DEFAULT_VAL_INDEX] = src_dir
                ARG_OPTIONS['work_dir'][DEFAULT_VAL_INDEX] = work_dir
            except KeyError:
                pass
        else:
            self.process = None

            # Set run ID based on working directory, for command-line operation
            if not self.parser.run_id and self.parser.work_dir:
                run_id = os.path.basename(os.path.realpath(run_dir))
                ARG_OPTIONS['run_id'][DEFAULT_VAL_INDEX] = run_id

        print "START  [" + self.task_name + "]"

        if info_str:
            self.info(info_str)


    def info(current_job, status):
        if self.process:
            self.process.get(force=True)
            self.process.udf[nsc.JOB_STATUS_UDF] = "Running ({0})".format(status)
            self.process.put()
        print "STATUS [" + self.task_name + "] " + status


    def fail(process, message, extra_info = None):
        """Report failure.
        
        NOTE: Calls sys.exit(1) to terminate program.
        
        (this was considered a more convenient protocol at the time of 
        writing this code)"""

        self.finished = True
        if self.process:
            self.process.get(force=True)
            self.process.udf[nsc.JOB_STATUS_UDF] = "Failed: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + ": " + message
            self.process.udf[nsc.JOB_STATE_CODE_UDF] = 'FAILED'
            if extra_info:
                self.process.udf[nsc.ERROR_DETAILS_UDF] = extra_info
            self.process.put()
        print "ERROR  [" + self.task_name + "]" + message
        if extra_info:
            print "----------"
            print extra_info
            print "----------"
        sys.exit(1)


    def success_finish(process):
        """Notify LIMS or command line that the job is completed.
        
        NOTE: Calls sys.exit(0) to terminate program."""

        self.finished = True
        complete_str = 'Completed successfully ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        if self.process:
            self.process.get(force=True)
            self.process.udf[nsc.JOB_STATUS_UDF] = complete_str
            self.process.udf[nsc.JOB_STATE_CODE_UDF] = 'COMPLETED'
            self.process.put()
            #TODO : would have some processing status UDF

        print "SUCCESS[" + self.task_name + "]" + complete_str
        sys.exit(0)



