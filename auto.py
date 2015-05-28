#------------------------------#
# Conveyor
# NSC data processing manager
#------------------------------#

# This script monitors the LIMS via the REST API and starts processing 
# steps. Its main purpose is to start other jobs. It only updates a small 
# number of user defined fields (UDFs) in the API to manage the processing 
# status. 

# This system doesn't have any storage system of its own -- all runtime
# data are kept in the LIMS.

import sys
import time
import logging
from argparse import ArgumentParser
from collections import defaultdict

# scilife genologics library
from genologics.lims import *

#from genologics.epp import EppLogger

# Local
from common import nsc, utilities

nsc.lims.check_version()


def mark_flowcell_projects(fc):
    """Sets a UDF for automatic processing on the flow cell Container.
        The "Automation lane groups" UDF on the container is set to a 
        comma separated list of the LIMSIDs of all pools in a single 
        project"""

    logging.debug("Processing flowcell " + fc.id)

    project_lanes = defaultdict(list)
    for lane,pool in fc.placements.items():
        project = None

        # Check that all samples in the pool (lane) are from the same
        # project
        for sample in pool.samples:
            if not project:
                project = sample.project
            else:
                if project.id != sample.project.id:
                    logging.error("Pool has samples from multiple projects. Skipping pool.")
                    project = None
                    break
            
        if project:
            project_lanes[project.id].append(lane)

    logging.debug("Marking %d groups of lanes as projects." % len(project_lanes))
    group_strings = []
    for project, lanes in project_lanes.items():
        lane_group = ",".join(sorted(lanes))
        group_strings.append(lane_group)

    lane_group_list = "|".join(group_strings)
    fc.udf[nsc.AUTO_FLOWCELL_UDF] = lane_group_list
    fc.put()
    


def set_qc_flags(process):
    for ana in process.all_inputs(unique=True):
        ana.get()
        ana.qc_flag = "PASSED"
        ana.put()



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
        logging.debug("Set the QC flags for process: " + process.id)
        set_qc_flags(process)
        logging.debug("Marking it for automation...")

        # Mark the flow cell for automatic processing (all sequencer types)
        mark_flowcell_projects(process.all_inputs()[0].location[0])

        logging.debug("Finishing sequencing step...")
        # Finish the sequencing step
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



def get_input_groups(queue):
    """Get groups of inputs for automated jobs.

    Flow cells which are set up for automation include a UDF with information 
    about which lanes should be processed together. The basic data items are 
    the positions of the lanes on the flow cell as given in the LIMS. The positions
    in a group are separated by commas. The groups are separated by pipe characters.

    Example: 1:1,2:1,3:1|4:1,5:1|6:1,7:1,8:1

    The groups correspond to proects.
    """

    # flowcell_info value tuples (flowcell, [analytes])
    flowcell_info = {}

    for ana in queue:
        fc = ana.location[0]
        if not flowcell_info.has_key(fc.id):
            flowcell_info[fc.id] = (fc, [], [])
        flowcell_info[fc.id][1].append(ana)

    groups = []

    for fcid,info in flowcell_info.items():
        fc = info[0]
        try:
            lanes_auto = fc.udf[nsc.AUTO_FLOWCELL_UDF]
        except KeyError:
            continue

        for group in lanes_auto.split("|"):
            have_lanes = [a.location[1] for a in info[1]]
            have_all = all(lane in have_lanes for lane in group.split(","))
            if have_all:
                groups.append([a for a in info[1] if a.location[1] in group])

    return groups


def get_input_flowcell(queue):
    """ Get a list of inputs from the queue of a step, 
    corresponding to all the pools on a  flow cell. If not all inputs are 
    present in the queue, the flow cell is ignored for now.
    
    If there are pools on the flow
    cell which do not have the automation flag set, these inputs are 
    ignored, and the flow cell may still be returned.
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
        fc = inputs.location[0]

        auto_udf = fc.udf[nsc.AUTO_FLOWCELL_UDF]
        lanes = auto_udf.replace("|", ",").split(",")
        have_input_ids = [i.id for i in inputs]
        
        valid = all(fc.placements[i].id in have_input_ids for i in lanes)

        if valid:
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
    """Checks for samples in the automated protocols and starts steps if 
    possible, then executes a script on the "record details" screen if 
    configured."""
    
    # Loop over protocols in NSC configuration file
    for protocol, protocol_steps in nsc.AUTOMATED_PROTOCOL_STEPS:
        proto = lims.get_protocols(name=protocol)[0]
        
        # Check all protocol steps known for this protocol
        for ps in proto.steps:
            
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
    logging.basicConfig(level=logging.DEBUG)
    logging.debug("auto.py Workflow management script")
    check_new_processes(nsc.lims)
    start_automated_protocols(nsc.lims)


