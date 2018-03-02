#--------------------------------------#
# Utilities for LIMS status tracking   #
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
    elif re.match(r"\d{6}_[A-Z0-9]", run_id):
        return 'hiseq'
    else:
        return None


def get_bcl2fastq2_version(work_dir):
    """Check version in log file in standard location (for non-LIMS).
    
    Less than bullet proof way to get bcl2fastq2 version."""

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
    else:
        return None
    

def get_fcid_by_runid(run_id):
    runid = get_instrument_by_runid(run_id)
    if runid.startswith("hiseq") or runid == "nextseq":
        return re.match(r"[\d]{6}_[\dA-Z]+_[\d]+_[AB]([A-Z\d-]+)$", run_id).group(1)
    else:
        return re.match(r"[\d]{6}_[\dA-Z]+_[\d]+_([A-Z\d-]+)$", run_id).group(1)

def merged_lanes(run_id):
    return get_instrument_by_runid(run_id) == "nextseq"


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





