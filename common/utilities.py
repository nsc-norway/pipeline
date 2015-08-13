#--------------------------------------#
# Utilities for LIMS status tracking   #
#--------------------------------------#

# Various utility functions for managing workflow progress,
# navigating between processes, and status tracking.

import subprocess
import sys
import datetime
import traceback
import re
import requests
import locale # Not needed in 2.7, see display_int
from xml.etree import ElementTree

from genologics.lims import *
import nsc


def get_sequencing_process(process):
    """Gets the sequencing process from a process object corresponing to a process
    which is run after sequencing, such as demultiplexing. This function looks up
    the sequencing step by examining the sibling processes run on one of the
    samples in the process's inputs."""

    # Each entry in input_output_maps is an input/output specification with a single
    # input and any number of outputs. This gets the first input.
    first_io = process.input_output_maps[0]
    first_in_artifact = first_io[0]['uri']

    processes = process.lims.get_processes(inputartifactlimsid=first_in_artifact.id)

    for proc in processes:
        if proc.type.name in [p[1] for p in nsc.SEQ_PROCESSES]:
            return proc


def get_instrument(seq_process):
    return next(p[0] for p in nsc.SEQ_PROCESSES if seq_process.type.name == p[1])


def get_instrument_by_runid(run_id):
    if re.match("\d{6}_M"):
        return 'miseq'
    elif re.match("\d{6}_NS"):
        return 'nextseq'
    else:
        return 'hiseq'


def merged_lanes(run_id):
    return get_instrument_by_runid(run_id) == "nextseq"


def logfile(process, step, command, extension="txt"):
    """Create the DemultiplexLogs dir if it doesn't exist. Returns the
    path to the log file for the step and command. One dir is created
    for each process ID.
    step: Name of the step, becomes part of the log name.
    command: Name of the command executed, also becomes part of the log name."""
    d1 = os.path.join(process.udf[WORK_RUN_DIR_UDF], "DemultiplexLogs")
    d2 = os.path.join(process.udf[WORK_RUN_DIR_UDF], "DemultiplexLogs", process.id)
    for d in [d1, d2]:
        try:
            os.mkdir(d)
        except OSError:
            pass
    return "{0}/{1}.{2}.{3}".format(
            d2, step.lower().replace(" ","_"),
            command.split("/")[-1],
            extension
            )


def get_index_sequence(artifact):
    for label in artifact.reagent_labels:
        # The structure of the reagent label XML isn't consistent with other lims
        # objects: there is no parent tag that holds all other data. Let's do an
        # ad hoc get request for now.
        lims = artifact.lims
        list_root = lims.get(lims.get_uri('reagenttypes'), params={'name': label})

        # Gets a list of reagent types with that name
        for rt in list_root.findall('reagent-type'):
            # Gets the reagent type by ID
            rt_root = lims.get(rt.attrib['uri'])

            # Look for the index in the XML hierarchy
            for special_type in rt_root.findall('special-type'):
                if special_type.attrib['name'] == "Index":
                    for attribute_tag in special_type.findall('attribute'):
                        if attribute_tag.attrib['name'] == "Sequence":
                            return attribute_tag.attrib['value']

        return None


def get_sample_sheet(process):
    """Downloads the demultiplexing process's sample sheet and returns
    the data."""

    sample_sheet = None
    for o in process.all_outputs(unique=True):
        if o.output_type == "ResultFile" and o.name == "SampleSheet csv":
            if len(o.files) == 1:
                return o.files[0].download()
    else:
        return None


def upload_file(process, name, path = None, data = None):
    """This function uploads the provided file to a ResultFile output with 
    on the given process with the specified name."""

    if not data:
        if path:
            data = open(path).read()
        else:
            raise ValueError("Must give either path or data")

    attach = None
    for out in process.all_outputs():
        if out.name == name:
            attach = out
            break
    if not attach:
        raise ValueError(name + " is not a valid result file for " + process.id)
    pf = ProtoFile(process.lims, attach, path)
    pf = process.lims.glsstorage(pf)
    f = pf.post()
    process.get(force=True)
    f.upload(data)


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


def get_udf(process, udf, default):
    try:
        return process.udf[udf]
    except KeyError:
        if not default is None:
            process.udf[udf] = default
            process.put()
        return default


class error_reporter():
    """Context manager for reporting error status when exceptions occur"""

    def __init__(self, process_id = None):
        self.process_id = process_id

    def __enter__(self):
        pass

    def __exit__(self, etype, value, tb):
        if etype is None:
            return True
        elif etype == SystemExit:
            return False

        if not self.process_id:
            if len(sys.argv) == 2:
                self.process_id = sys.argv[1]

        if self.process_id:
            process = Process(nsc.lims, id=self.process_id)
            fail(process, etype.__name__ + " " + str(value),
                    "\n".join(traceback.format_exception(etype, value, tb)))

        return False # re-raise exception


def get_sample_sheet_proj_name(seq_process, project):
    """Get the project name as it would appear in the sample sheet.
    Will become really complex if we allow other than [A-Za-z0-9\-] in 
    sample sheet."""
    return project.name


def get_num_reads(run_dir):
    """Get the number of read passes from the RunInfo.xml file.     

    1=single read, 2=paired end.
    Also returns the number of index reads.

    Returns a tuple: (number of data reads, number of index reads)
    """

    run_info = ElementTree.parse(os.path.join(run_dir, "RunInfo.xml")).getroot()
    reads = run_info.find("Run").find("Reads")
    n_data, n_index = 0, 0
    for read in reads.findall("Read"):
        if read.attrib['IsIndexedRead'] == 'Y':
            n_index += 1
        else:
            n_data += 1

    return n_data, n_index



locale.setlocale(locale.LC_ALL, 'en_US')
def display_int(val):
    """Adds thousands separators. To be replaced with "{:,}".format(val) when 
    upgrading to Python 2.7"""
    return locale.format("%d", round(val), grouping=True)


# The check_output function is only available in Python >=2.7, but we also support 2.6,
# as on RHEL6.
if sys.version_info >= (2, 7):
    check_output = subprocess.check_output
else:
    def check_output(args, **kwargs):
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
        data = proc.communicate()

        rcode = proc.wait()
        if rcode == 0:
            return data[0]
        else:
            raise subprocess.CalledProcessError(rcode, args[0] + ": " +str(rcode) +  data[1])





