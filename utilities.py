#------------------------------#
# Workflow management          #
#------------------------------#

# Manages workflow steps in the LIMS. This mainly centers around the 
# step API resource. Completing and starting steps.


import logging
import subprocess, sys
import datetime
from genologics.lims import *
import nsc

logger = logging.getLogger()


def finish_step(lims, process_id):
    '''Sets next step for all analytes and finishes the specified step.
    This function can handle steps which have a single configured next
    step, and steps which are the last step of a protocol.
    All samples are advanced to the next step.'''

    step = Step(lims, id=process_id)

    if step.current_state != 'Completed':

        next_elements = step.configuration.transitions
        finish_protocol = len(next_elements) == 0
        if len(next_elements) == 1:
            next_step = next_elements[0].step
        else:
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
    '''Gets the sequencing process from a process object corresponing to a process
    which is run after sequencing, such as demultiplexing. This function looks up
    the sequencing step by examining the sibling processes run on one of the
    samples in the process's inputs.'''

    # Each entry in input_output_maps is an input/output specification with a single
    # input and any number of outputs. This gets the first input.
    first_io = process.input_output_maps[0]
    first_in_artifact = first_io[0]['uri']

    processes = process.lims.get_processes(inputartifactlimsid=first_in_artifact.id)

    for proc in processes:
        if proc.type.name in [p[1] for p in nsc.SEQ_PROCESSES]:
            return proc


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


def upload_file(process, name,  path):
    attach = None
    for out in process.all_outputs():
        if out.name == name:
            attach = out
            break
    if not attach:
        raise ValueError(name + " is not a valid result file for " + process.id)
    pf = ProtoFile(process.lims, attach.uri, path)
    pf = process.lims.glsstorage(pf)
    f = pf.post()
    process.get(force=True)
    f.upload(open(path).read())


def running(process):
    process.udf[nsc.JOB_STATUS_UDF] = 'Running'
    process.put()


def fail(process, message):
    '''Report failure from background job'''

    process.udf[nsc.JOB_STATUS_UDF] = 'Failed: ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + ": " + message
    process.put()

def success_finish(process):
    '''Called by background jobs (slurm) to declare that the task has been 
    completed successfully.'''

    process.udf[nsc.JOB_STATUS_UDF] = 'Completed successfully ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    process.put()

    try:
        automation = process.all_inputs()[0].udf[nsc.AUTO_FLOWCELL_UDF]
    except KeyError:
        automation = False

    if automation:
        finish_step(process.lims, process.id)


# The check_output function is only available in Python >=2.7, but we also support 2.6,
# as on RHEL6.
if sys.version_info >= (2, 7):
    check_output = subprocess.check_output
else:
    def check_output(args):
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        data = proc.communicate()

        rcode = proc.wait()
        if rcode == 0:
            return data[0]
        else:
            raise OSError(args[0] + ": " +str(rcode) +  data[1])



