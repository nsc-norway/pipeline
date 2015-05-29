#--------------------------------------#
# Utilities for LIMS status tracking   #
#--------------------------------------#

# Various utility functions for managing workflow progress,
# navigating between processes, and status tracking.

import logging
import subprocess
import sys
import time
import datetime
import traceback
import requests
import locale # Not needed in 2.7, see display_int
from operator import attrgetter
from genologics.lims import *
import nsc

logger = logging.getLogger()


def finish_step(lims, process_id):
    """Sets next step for all analytes and finishes the specified step.
    This function can handle steps which have a single configured next
    step, and steps which are the last step of a protocol.
    All samples are advanced to the next step."""

    step = Step(lims, id=process_id)

    if step.current_state != 'Completed':

        next_elements = step.configuration.transitions
        finish_protocol = len(next_elements) == 0
        if len(next_elements) == 1:
            next_step = next_elements[0].step
        elif len(next_elements) > 1:
            raise Exception("There are multiple options for next step, don't know what to do.")
    
        # Set next action for each sample
        for a in step.actions.next_actions:
            if finish_protocol:
                a.action = "complete"
            else:
                a.action = "nextstep"
                a.next_step = next_step
        step.actions.put()
    
        while step.current_state in ['Record Details', 'Assign Next Steps']:
            step.advance()
    
        if step.current_state != 'Completed':
            raise Exception("Failed to finish the step. It is in state " + step.current_state)



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


def get_demux_process(process):
    """Gets the demultiplexing process corresponding to a given analyte (lane
    on a flow cell). Analogous to the above method."""

    # Find the demultiplexing process: First process which has output file
    # per reagent label
    input = process.all_inputs()[0]
    sib_procs = nsc.lims.get_processes(inputartifactlimsid=input.id)
    sib_procs.sort(key=attrgetter('id'))
    for s in sib_procs:
        io = s.input_output_maps
        for x in io:
            output = x[1]
            if output['output-type'] == "ResultFile" and\
                    output['output-generation-type'] == "PerReagentLabel":
                return s
    

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
    pf = ProtoFile(process.lims, attach, path)
    pf = process.lims.glsstorage(pf)
    f = pf.post()
    process.get(force=True)
    f.upload(data)


def running(process, status = None):
    for i in xrange(100): # Prevent race condition with epp-submit-slurm.py
                          # and always refresh process to get the QC flags
        process.get(force=True)
        try:
            job_id = process.udf[nsc.JOB_ID_UDF]
            if job_id:
                break
        except KeyError:
            pass
        time.sleep(2)

    if status:
        process.udf[nsc.JOB_STATUS_UDF] = "Running ({0})".format(status)
    else:
        process.udf[nsc.JOB_STATUS_UDF] = "Running"
    process.udf[nsc.JOB_STATE_CODE_UDF] = 'RUNNING'
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

def success_finish(process, do_finish_step=True):
    """Called by background jobs (slurm) to declare that the task has been 
    completed successfully."""

    process.get(force=True)
    process.udf[nsc.JOB_STATUS_UDF] = 'Completed successfully ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    process.udf[nsc.JOB_STATE_CODE_UDF] = 'COMPLETED'
    process.put()

    if do_finish_step:
        try:
            automation = process.all_inputs()[0].location[0].udf[nsc.AUTO_FLOWCELL_UDF]
        except KeyError:
            automation = False

        if automation:
            finish_step(process.lims, process.id)


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
            raise subprocess.CalledProcessError(args[0] + ": " +str(rcode) +  data[1])





