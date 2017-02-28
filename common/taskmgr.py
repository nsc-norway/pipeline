
from __future__ import print_function

import os
import sys
import traceback
import argparse
import datetime
import time

# local
import utilities
import samples
import nsc

from genologics.lims import *


# Module to manage processing status for all the script, and to abstract the 
# differences between command line invocation and invocation through LIMS.


# Standard argument definitions
# { name => (argparse_name, udf_name, type, default, description) }
# sample_sheet doesn't have UDF, special case
# Note: in the argument parser we attempt to set the name of the attribute on the result
# object equal to the "name" of the option, i.e. the key of this dict. For positional 
# arguments (without leading "--"), however, the attribute will be called by the argparse_name
# instead. So keep name and argparse_name equal for those.
ARG_OPTIONS = {
        "src_dir": ["src_dir", nsc.SOURCE_RUN_DIR_UDF, str, None, "Source directory (run folder)"],
        "work_dir": ["work_dir", nsc.WORK_RUN_DIR_UDF, str, None, "Destination/working directory (run folder)"],
        "run_id": ["--run-id", nsc.RUN_ID_UDF, str, None, "Override run ID (mostly useless)"],
        "threads": ["--threads", nsc.THREADS_UDF, int, 16, "Number of threads/cores to use"],
        "sample_sheet": ["--sample-sheet", None, str, "<DIR>/DemultiplexingSampleSheet[_LXYZ].csv", "Sample sheet"],
        "lanes": ["--lanes", nsc.LANES_UDF, str, None, "Lanes to process (default: all)"],
        }
DEFAULT_VAL_INDEX = 3

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

    def __init__(self, task_name, task_description, arg_names):
        self.task_name = task_name
        self.task_description = task_description
        self.arg_names = arg_names
        self.script_name = os.path.basename(sys.argv[0])
        self.parser = argparse.ArgumentParser(description=task_description)
        self.parser.add_argument("--pid", dest="pid", default=None, help="Process ID if running within LIMS")
        self.args = None # To be set when argument parser is run
        self.process = None
        self.finished = False
        self.success = False
        self.message = ""


    def get_arg(self, arg_name):
        argparse_name, udf_name, type, default, help = ARG_OPTIONS[arg_name]
        if self.process:
            return utilities.get_udf(self.process, udf_name, default)
        else:
            val = getattr(self.args, arg_name)
            if val is None: # Handle when default gets updated
                return default
            else:
                return val

    @property
    def run_id(self):
        if self.process:
            return self.get_arg("run_id")
        elif "work_dir" in self.arg_names:
            return os.path.basename(os.path.realpath(self.args.work_dir))
        elif "src_dir" in self.arg_names:
            return os.path.basename(os.path.realpath(self.args.src_dir))
        else:
            raise RuntimeError("Can't get the run ID based on the run folder dir")

    @property
    def work_dir(self):
        return self.get_arg('work_dir')

    @property
    def bc_dir(self):
        return os.path.join(self.work_dir, "Data", "Intensities", "BaseCalls")

    @property
    def src_dir(self):
        return self.get_arg('src_dir')

    @property
    def threads(self):
        return self.get_arg('threads')

    @property
    def sample_sheet_content(self):
        """Get the content of the "demultiplexing" sample sheet"""
        if self.process:
            sample_sheet = None
            for o in self.process.all_outputs(unique=True):
                if o.output_type == "ResultFile" and o.name == nsc.SAMPLE_SHEET:
                    if len(o.files) == 1:
                        sample_sheet = o.files[0].download()
                        break

        else:
            sample_sheet = open(self.sample_sheet_path, 'rb').read()

        return sample_sheet

    @property
    def sample_sheet_path(self):
        """Get the path to the sample sheet, for non-lims operation only"""

        path = self.args.sample_sheet
        if path == "<DIR>/DemultiplexingSampleSheet[_LXYZ].csv": # This is the default
            options = [
                        "DemultiplexingSampleSheet" + self.suffix + ".csv",
                        "DemultiplexingSampleSheet.csv",
                        "SampleSheet.csv"
                        ]

            for filename in options:
                path = os.path.join(self.work_dir, filename)
                if os.path.exists(path):
                    break

        return path

    
    def logfile(self, command, extension="txt"):
        """Get the path to a logfile in the standard log directory, for use with
        subprocesses, etc."""

        logdir = os.path.join(self.work_dir, nsc.RUN_LOG_DIR)

        if not os.path.exists(logdir):
            os.mkdir(logdir)

        if self.process:
            return os.path.join(
                    logdir,
                    "{0}.{1}.{2}.{3}".format(
                        self.task_name.lower().replace(" ","_"),
                        self.process.id,
                        command.split("/")[-1],
                        extension
                        )
                    )
        elif self.suffix:
            return os.path.join(
                    logdir,
                    "{0}.{1}.{2}.{3}".format(
                        self.task_name.lower().replace(" ","_"),
                        self.suffix,
                        command.split("/")[-1],
                        extension
                        )
                    )
        else:
            return os.path.join(
                    logdir,
                    "{0}.{1}.{2}".format(
                        self.task_name.lower().replace(" ","_"),
                        command.split("/")[-1],
                        extension
                        )
                    )

    @property
    def projects(self):
        """Get the list of project objects, defined in the samples module. """

        num_reads, index_reads = utilities.get_num_reads(self.work_dir)
        sample_sheet = samples.parse_sample_sheet(self.sample_sheet_content)
        sample_sheet_data = sample_sheet['data']

        # Supply a list of lanes if lane number isn't given in the sample sheet
        expand_lanes = None
        if not self.no_lane_splitting:
            instr = utilities.get_instrument_by_runid(self.run_id)
            if instr == "nextseq":
                expand_lanes = [1,2,3,4]
            elif instr == "miseq":
                expand_lanes = [1]
            else:
                expand_lanes = None

        experiment_name = None
        if sample_sheet.has_key('header'):
            experiment_name = sample_sheet['header'].get("Experiment Name")

        return samples.get_projects(
                self.run_id,
                sample_sheet_data,
                num_reads,
                self.no_lane_splitting,
                expand_lanes,
                experiment_name,
                self.lanes
                )

    @property
    def no_lane_splitting(self):
        """Gets the no-lane-splitting AKA merged lanes option.
        
        This is used by tasks running after demultiplexing, to determine
        whether data from multiple lanes are combined into single files. """
        if self.process:
            return utilities.get_udf(self.process, nsc.NO_LANE_SPLITTING_UDF, False)
        else:
            return samples.check_files_merged_lanes(self.work_dir)
    
    @property
    def lanes(self):
        """List of lanes to process, specified in LIMS or on the command line.
        
        Returns None if all lanes should be processed."""
        try:
            lanes = self.get_arg("lanes")
            if lanes:
                return [int(l) for l in lanes] # Convert str to list of int
            else:
                return None
        except AttributeError:
            return None # None = all

    @property
    def suffix(self):
        """Returns a suffix to append to certain files / diretories to allow running
        multiple demultiplexing jobs with different options. Used for Stats/, Reports/,
        SampleSheet, ... 
        
        For files, it will typically be spliced in before the extension."""

        if self.lanes:
            return "_L" + "".join([str(c) for c in self.lanes])
        else:
            return ""

    @property
    def instrument(self):
        return utilities.get_instrument_by_runid(self.run_id)

    # Arguments:
    def add_argument(self, *args, **kwargs):
        """Add an extra argument for command line. Used for args which
        are only applicable for a single task, and not part of the 
        general ones defined above.
        
        Essentially, we bypass this class and interact directly with the
        ArgumentParser."""
        self.parser.add_argument(*args, **kwargs)


    # Context manager protocol: __enter__ and __exit__
    def __enter__(self):
        return self


    def __exit__(self, etype, value, tb):
        """Catch unexpected exceptions and also throw an error if exiting without
        success_finish. Called when exiting the with ... section.

        Note: many exit points in this function.
        """

        if self.finished:
            return self.success

        if etype is None:
            print("Uexpected exit", file=sys.stderr)
            if self.process:
                utilities.fail(self.process, "Unexpected exit", "Unexpected exit without exception")

        # Note: fail() function calls exit()
        self.fail(etype.__name__ + " " + str(value),
                "\n".join(traceback.format_exception(etype, value, tb)))

        return False



    # To be called to indicate the status
    def running(self, info_str=None):
        """Users should call this function once at the start of the task to 
        indicate that the program has started.

        parse_args() will exit() if the args are incorrect.
        
        The reason that this function exists, and that it's not handled
        by the constructor or __enter__, is that the script has a chance
        to set extra command line arguments before running running().
        This function can then use those arguments to populate the Task
        internal state."""

        # Initialization code common to all tasks:
        os.umask(007)

        # Argument setup
        for name in self.arg_names:
            argparse_name, udf_name, type, default, help = ARG_OPTIONS[name]
            if argparse_name.startswith("--"):
                self.parser.add_argument(
                        argparse_name,
                        dest=name,
                        type=type,
                        default=default,
                        help=help
                        )
            else: # we're not allowed to set dest for positional arguments
                self.parser.add_argument(
                        argparse_name,
                        type=type,
                        default=default,
                        help=help,
                        nargs="?"
                        )

        try:
            self.args = self.parser.parse_args()
        except SystemExit:
            self.finished = True
            raise

        if not self.args.pid and not self.args.work_dir:
            self.fail("Missing required options", 
                    "You must specify either the LIMS process ID (--pid) or the working directory. " +
                    "Use -h option for usage info.")

        if self.args.pid:
            # LIMS operation
            self.process = Process(nsc.lims, id=self.args.pid)
            self.process.get()
            self.process.udf[nsc.JOB_STATUS_UDF] = "Running"
            self.process.udf[nsc.JOB_STATE_CODE_UDF] = 'RUNNING'
            self.process.udf[nsc.CURRENT_JOB_UDF] = self.task_name
            self.process.udf[nsc.ERROR_DETAILS_UDF] = ""
            self.process.put()

            # Set defaults for source & working directories based on run ID
            # (only available for LIMS)
            run_id = None
            try:
                run_id = self.process.udf[nsc.RUN_ID_UDF]
            except KeyError: # Run ID not set on process, try sequencing process
                sequencing_process = utilities.get_sequencing_process(self.process)
                if sequencing_process:
                    try:
                        run_id = sequencing_process.udf['Run ID']
                    except KeyError:
                        pass

            if run_id:
                src_dir = os.path.join(nsc.PRIMARY_STORAGE, run_id)
                work_dir = os.path.join(nsc.SECONDARY_STORAGE, run_id)
                # These defaults are set to None in the ARG_OPTIONS initialization,
                # no need to check if they are None
                ARG_OPTIONS['run_id'][DEFAULT_VAL_INDEX] = run_id
                ARG_OPTIONS['src_dir'][DEFAULT_VAL_INDEX] = src_dir
                ARG_OPTIONS['work_dir'][DEFAULT_VAL_INDEX] = work_dir
            else:
                self.fail("Run ID not found!")
        else:
            self.process = None

        print("START  [" + self.task_name + "] " + self.script_name, file=sys.stderr)

        if info_str:
            self.info(info_str)


    def info(self, status):
        self.safe_lims_update("Running ({0})".format(status))
        print("INFO   [" + self.task_name + "] " + status, file=sys.stderr)


    def array_job_status(self, array_jobs):
        """Shpw progress information for array jobs (#Pending/Running/Completed...) as 
        INFO message if it is changed since last invocation of this method."""
        info_strings = []
        for array_job in array_jobs:
            known_codes = ['PENDING', 'RUNNING', 'FAILED', 'COMPLETED']
            states = [
                    "{0}:{1}".format(code[0:1], array_job.summary[code]) 
                    for code in known_codes
                    if array_job.summary.get(code)
                    ]
            other_states = sum(v for code, v in array_job.summary.items() if code not in known_codes) 
            if other_states:
                states.append("?:{0}".format(other_states))

            info_strings.append("[%s] " % array_job.job_id + ", ".join(states))
        self.info(" / ".join(info_strings))


    def job_status(self, job_id, job_name, status):
        new_message = "[{0}] {1} {2}".format(job_id, job_name, status)
        if new_message != self.message:
            self.message = new_message
            self.safe_lims_update(new_message)
            print("INFO   [" + self.task_name + "] " + new_message, file=sys.stderr)


    def warn(self, status):
        self.safe_lims_update("Running | Warning: {0}".format(status))
        print("WARN   [" + self.task_name + "] " + status, file=sys.stderr)


    def fail(self, message, extra_info = None):
        """Report failure.
        
        NOTE: Calls sys.exit(1) to terminate program.
        
        (this was considered a more convenient protocol at the time of 
        writing this code)"""

        self.finished = True
        self.success = False
        status = "Failed: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + ": " + message
        self.safe_lims_update(status, 'FAILED', extra_info)
        if extra_info:
            print("ERROR  [" + self.task_name + "] " + message, file=sys.stderr)
            print("-----------", file=sys.stderr)
            print(extra_info, file=sys.stderr)
            print("-----------", file=sys.stderr)
        print("ERROR  [" + self.task_name + "] " + message, file=sys.stderr)
        sys.exit(1)


    def success_finish(self):
        """Notify LIMS or command line that the job is completed.
        
        NOTE: Calls sys.exit(0) to terminate program."""

        self.finished = True
        self.success = True
        complete_str = 'Completed successfully ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        self.safe_lims_update(complete_str, 'COMPLETED')
        print("SUCCESS[" + self.task_name + "] " + complete_str, file=sys.stderr)
        sys.exit(0)


    def safe_lims_update(self, message, state_code=None, error_details=None, force=False):
        if self.process:
            started = time.time()
            while time.time() < started + 4*24*3600:
                try:
                    self.process.get(force=force)
                    self.process.udf[nsc.JOB_STATUS_UDF] = message
                    if state_code is not None:
                        self.process.udf[nsc.JOB_STATE_CODE_UDF] = state_code
                    if error_details is not None:
                        self.process.udf[nsc.ERROR_DETAILS_UDF] = error_details
                    self.process.put()
                    break
                except Exception, e:
                    force = True
                    time.sleep(300) # Try every 5 minutes
            else:
                # If we didn't break out, we timed out
                raise e

