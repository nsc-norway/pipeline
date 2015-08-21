#------------------------------#
# Conveyor
# NSC data processing manager
#------------------------------#

# This script monitors the LIMS via the REST API and starts processing 
# steps. Its main purpose is to start other jobs. It only updates a small 
# number of user defined fields (UDFs) in the API to manage the processing 
# status. This system doesn't have any storage system of its own -- all
# runtime data are kept in the LIMS.

# The new version performs these primary tasks:
# * Checks the queue of the demultiplexing/QC step, and starts a task
#   for each full flowcell which is present, which also has the automation
#   flag on the sequencing process, and the sequencing is finsihed.
# * For each demultiplexing/QC step with the automation flag set, checks 
#   whether a program is running, and starts a new one if appropriate (TBD).


# This script is a TODO, sort of the last piece of the puzzle

import sys
import time
import logging
from collections import defaultdict

# scilife genologics library
from genologics.lims import *

# Local
from common import nsc, utilities

nsc.lims.check_version()


def init_automation(lims, instrument, process):
    """This should be called on sequencing processes which already have
    the automation flag set. This clears the process-level automation flag 
    and sets UDFs on the container instead. It will only run if all
    QC flags are set."""

    # Is the sequencing finished?
    finished = False
    try:
        #finished = process.udf['Dummy date field']
        finished = process.udf["Finish Date"] != ""
    except KeyError:
        pass

    if finished:
        logging.debug("Sequencing is finished for process: " + process.id)

        logging.debug("Marking flowcell for automation...")
        # Mark the flow cell for automatic processing (all sequencer types)
        mark_flowcell_projects(process.all_inputs()[0].location[0])

        # Finish the sequencing step for NextSeq, which has data analysis in the
        # same workflow
        if instrument == "nextseq":
            logging.debug("Set the QC flags for process: " + process.id)
            set_qc_flags(process)
            logging.debug("Finishing sequencing step...")
            utilities.finish_step(lims, process.id)

        # Automated processing now triggered, can remove flag so we don't 
        # have to process it again.
        logging.debug("Done. Removing automation checkbox.")
        process.udf[nsc.AUTO_FLAG_UDF] = False
        process.put()
        logging.debug("Finished marking for automation")

    else:
        logging.debug("Sequencing is not yet finished for process: " + process.id)



def check_new_processes(lims): 
    """Query the API for new sequencing processes with automation flag"""
    
    for instr, process in nsc.SEQ_PROCESSES:
        ps = lims.get_processes(type = process, udf = {nsc.AUTO_FLAG_UDF: "true"})
        logging.debug("Found " + str(len(ps)) + " sequencing processes of type " + process)
        for p in ps:
            init_automation(lims, instr, p)




def get_input_flowcell(queue):
    """ Get a list of inputs from the queue of a step, 
    corresponding to all the analytes on a flow cell. If not all inputs are 
    present in the queue, the flow cell is ignored for now.
    """

    flowcell_inputs = defaultdict(list)

    for ana in queue:
        fcid = ana.location[0].id
        input_list = flowcell_inputs[fcid]
        input_list.append(ana)

    logging.debug("Found %d flowcells, checking if any are complete" % len(flowcell_inputs))

    flowcell_groups = []
    for fcid,inputs in flowcell_inputs.items():
        logging.debug("Checking flowcell " + fcid)

        automation = False
        seq_processes = process.lims.get_processes(inputartifactlimsid=inputs[0].id)
        for proc in processes:
            if proc.type.name in [p[1] for p in nsc.SEQ_PROCESSES]:
                try:
                    automation = proc.udf[nsc.AUTO_FLAG_UDF]
                    break
                except KeyError:
                    automation = False
        else: # if not break
            automation = False

        do_process = False
        if automation:
            if frozenset(inputs) == frozenset(proc.all_inputs()):
                do_process = True

        if do_process:
            flowcell_groups.append(inputs)
            logging.debug("All expected inputs are present, returning this flow cell")
        else: 
            logging.debug("Not all inputs found in flow cell, ignoring it")

    return flowcell_groups


def start_steps(lims, protocol_step, protocol_step_setup):
    """Starts zero or more instances of a specific protocol step.
    
    protocol_step is a protocol step configuration entity from the LIMS.
    
    protocol_step_setup is a NSC-specific class which holds a few configuration 
    options."""

    queue = protocol_step.queue()
    logging.debug("Found %d items in the queue" % (len(queue.artifacts)))
    if protocol_step_setup.grouping == "project":
        jobs = get_input_groups(queue.artifacts)
    elif protocol_step_setup.grouping == "flowcell":
        jobs = get_input_flowcell(queue.artifacts)
    else:
        raise Exception("Grouping not configured correctly for " + protocol_step_setup.name)
    
    steps = []
    for job in jobs:
        logging.debug("Creating step for samples " + ",".join([ana.id for ana in job]))
        steps.append(lims.create_step(protocol_step, job))

    return steps



def start_automated_protocols(lims):
    """Checks for samples in the queue for the demultiplexing script, and
    starts them if everything is set up correctly."""
    
    proto = lims.get_protocols(name=nsc.DEMULTIPLEXING_QC_PROTOCOL)[0]
    
    step = next(step for step in proto.steps if 
    # Check all protocol steps known for this protocol
    for ps in proto.steps:
            




#### Old code...
            
            # Check if this protocol step should be processed 
            found = False
            for setup in protocol_steps:
                if setup.name == ps.name:
                    found = True
                    break

            if found:
                logging.debug("Checking the queue of " + setup.name)
                steps = start_steps(lims, ps, setup)
                logging.debug("Started %d steps." % len(steps))

                # Run scripts if configured
                if setup.script:
                    for step in steps:
                        # Wait for scripts to finish
                        any_running = True
                        while any_running:
                            any_running = False
                            step.get(force=True)
                            for prog_stat in step.program_status:
                                prog_stat.get(force=True)
                                if prog_stat.status in ("RUNNING", "QUEUED"):
                                    any_running = True
                            if any_running:
                                time.sleep(1)
                        # Run the program
                        for ap in step.available_programs:
                            if ap.name == setup.script:
                                ap.trigger()



if __name__ == "__main__":
    if nsc.TAG == "dev":
        logging.basicConfig(level=logging.DEBUG)
    logging.debug("auto.py Workflow management script")
    check_new_processes(nsc.lims)
    start_automated_protocols(nsc.lims)


