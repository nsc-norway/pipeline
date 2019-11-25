#--------------------------------------#
# Utility function                     #
#--------------------------------------#

# Various utility functions for managing workflow progress,
# navigating between processes, and status tracking.

import subprocess
import sys
import os
import datetime
import traceback
import re
import requests
import glob
import locale # Not needed in 2.7, see display_int
from collections import defaultdict
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
    seq_processes = [proc for proc in processes if proc.type_name in [p[1] for p in nsc.SEQ_PROCESSES]]
    try:
        # Use the last sequencing process. There may be more than one process if 
        # the sequencing step was repeated, but not the clustering step (this should
        # not be done)
        return seq_processes[-1]
    except IndexError:
        return None


def get_instrument(seq_process):
    return next(p[0] for p in nsc.SEQ_PROCESSES if seq_process.type_name == p[1])


def get_instrument_by_runid(run_id):
    if re.match(r"\d{6}_M", run_id):
        return 'miseq'
    elif re.match(r"\d{6}_N", run_id):
        return 'nextseq'
    elif re.match(r"\d{6}_E00401", run_id): # This machine was changed to a HiSeq 4000
        return 'hiseq4k'
    elif re.match(r"\d{6}_E", run_id):
        return 'hiseqx'
    elif re.match(r"\d{6}_[JK]", run_id):
        return 'hiseq4k'
    elif re.match(r"\d{6}_A", run_id):
        return 'novaseq'
    elif re.match(r"\d{6}_[A-Z0-9]", run_id):
        return 'hiseq'
    else:
        return None


def get_bcl2fastq2_version(process, work_dir):
    """Attemts to get bcl2fastq version using LIMS, then by inspecting
    the log file.
    
    If nothing works, it will raise a RuntimeError."""

    if process:
        try:
            return process.udf[nsc.BCL2FASTQ_VERSION_UDF]
        except KeyError:
            pass
    log_path_pattern = os.path.join(
            work_dir,
            nsc.RUN_LOG_DIR,
            "30._demultiplexing.*bcl2fastq2.txt"
            )
    log_paths = sorted(glob.glob(log_path_pattern))
    if log_paths:
        log_path = log_paths[-1]
        log = open(log_path)
        for i in xrange(3):
            l = next(log)
            if l.startswith("bcl2fastq v"):
                return l.split(" ")[1].strip("\n")
    raise RuntimeError("Unable to determine bcl2fastq version.")


def get_rta_version(run_dir):
    try:
        xmltree = ElementTree.parse(os.path.join(run_dir, 'RunParameters.xml'))
    except IOError:
        xmltree = ElementTree.parse(os.path.join(run_dir, 'runParameters.xml'))
    run_parameters = xmltree.getroot()
    rta_ver_element = run_parameters.find("RTAVersion")
    if rta_ver_element == None:
        rta_ver_element = run_parameters.find("RtaVersion")
    if rta_ver_element == None:
        rta_ver_element = run_parameters.find("Setup").find("RTAVersion")
    return rta_ver_element.text 


def get_fcid_by_runid(run_id):
    runid = get_instrument_by_runid(run_id)
    if runid.startswith("hiseq") or runid == "nextseq":
        return re.match(r"[\d]{6}_[\dA-Z]+_[\d]+_[AB]([A-Z\d-]+)$", run_id).group(1)
    else:
        return re.match(r"[\d]{6}_[\dA-Z]+_[\d]+_([A-Z\d-]+)$", run_id).group(1)

def merged_lanes(run_id):
    return get_instrument_by_runid(run_id) == "nextseq"


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
    pf = process.lims.glsstorage(attach, path)
    f = pf.post()
    process.get(force=True)
    f.upload(data)



def get_sample_sheet_proj_name(lims_project_name):
    """Get the project name as it would appear in the sample sheet.
    Will become really complex if we allow other than [A-Za-z0-9\-] in 
    sample sheet."""
    return re.sub(r'[^a-zA-Z0-9_\-]', '_', lims_project_name)


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


# LimsInfo class can be used to get information about a project!
# Used in 60_emails and 90_prepare_delivery, so moved here.
class LimsInfo(object):
    """Gets project information: contact person, etc., from UDFs in LIMS.
    Also identifies previous sequencing runs, and counts the number of lanes
    which are either PASSED, FAILED, or unknown."""
    def __init__(self, lims_project, seq_process):
        self.contact_person = lims_project.udf.get('Contact person')
        self.contact_email = lims_project.udf.get('Contact email')
        self.delivery_method = lims_project.udf.get('Delivery method')
        self.internal_bc_demultiplexing_16s = lims_project.udf.get(nsc.PROJECT_16S_UDF)
        self.total_number_of_lanes = lims_project.udf.get('Number of lanes')
        completed_runs = lims_project.lims.get_processes(
                type=(t[1] for t in nsc.SEQ_PROCESSES),
                projectname=lims_project.name
                )
        completed_lanes_all = sum(
                (run_process.all_inputs(unique=True)
                for run_process in completed_runs),
                []
                )
        completed_lanes = set(lane.stateless for lane in completed_lanes_all)
        lims_project.lims.get_batch(completed_lanes)
        lims_project.lims.get_batch(lane.samples[0] for lane in completed_lanes)
        state_count = defaultdict(int)
        this_run_lanes = set(seq_process.all_inputs())
        for lane in completed_lanes:
            if lane in this_run_lanes:
                state = "THIS_RUN"
            else:
                state = lane.qc_flag
            if lane.samples[0].project == lims_project:
                state_count[state]+=1
        self.status_map = state_count
        self.sequencing_status = ", ".join(str(k) + ": " + str(v) for k, v in state_count.items())


def get_udf(process, udf, default):
    try:
        return process.udf[udf]
    except KeyError:
        if not default is None:
            process.udf[udf] = default
            process.put()
        return default

def strip_chars(string):
    """Strips special characters to prevent unexpected behaviour or security issues when 
    user input is used as file names, or similar."""
    return "".join(c for c in string if c.isalnum() or c in '-_.')


# *** Compatibility support functions ***

# Locale setting is used for the function below
try:
    locale.setlocale(locale.LC_ALL, 'en_US')
except locale.Error:
    pass # Can't be sure we use correct thousands separator

def display_int(val):
    """Adds thousands separators. To be replaced with "{:,}".format(val) when 
    upgrading to Python 2.7"""
    if val is None:
        return "-"
    elif sys.version_info >= (2,7):
        return "{:,.0f}".format(val)
    else:
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





