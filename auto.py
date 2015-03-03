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
import logging
from argparse import ArgumentParser

# scilife genologics library
from genologics.lims import *

#from genologics.epp import EppLogger

# Local
import nsc
import workflow

logger = logging.getLogger()
nsc.lims.check_version()


def mark_project_pools(inputs):
    '''Sets a UDF for automatic processing on all pools
        The "Automatic processing group" UDF on the pools is set to a 
        comma separated list of the LIMSIDs of all pools in a single 
        project'''
    project_pools = {}
    for pool in inputs:
        project = None

        for sample in pool.samples:
            if not project:
                project = sample.project
            else:
                if project.id != sample.project.id:
                    logger.error("Pool has samples from multiple projects. Skipping pool.")
                    project = None
                    break
            
        if project:
            project_pools.get(project, default=[]).append(pool)

    for project,pools in project_pools:
        pool_id_list = ",".join(p.id for p in pools)
        for pool in pools:
            pool.udf[nsc.AUTO_POOL_UDF] = pool_id_list
            pool.put()


def 



def init_automation(instrument, process):
    '''This should be called on sequencing processes which already have
    the automation flag set. This clears the process-level automation flag 
    and sets UDFs on the analytes (pools) instead. It will only run if all
    QC flags are set.'''

    # Is the sequencing finished?
    if process.udf.get("Finish Date"):

        if qc_flags_set(process):
            # Mark the pools for automatic processing (all sequencer types)
            mark_project_pools(process.all_inputs())

            # Finish the sequencing step
            workflow.finish_process(process)

            # Automated processing now triggered, can remove flag so we don't 
            # have to process it again.
            process.udf[nsc.AUTO_FLAG_UDF] = False
            process.put()



def check_new_processes(lims): 
    '''Query the API for new sequencing processes with automation flag'''
    for instr, process in nsc.SEQ_PROCESSES:
        ps = lims.get_processes(type = process, udf = {nsc.AUTO_FLAG_UDF: "on"})
        for p in ps:
            init_automation(instr, p)



def get_input_groups(queue):
    '''Get group of inputs for automated jobs.

    Input is a list of artifacts.
    
    Analytes have a UDF which contains a comma separated list of 
    all the sample IDs of an automation group.
    The groups correspond to a projects. This function returns a 
    list of *complete* groups of inputs -- if not all samples are
    present, then none are returned.'''

    input_map = {}
    for ana in queue:
        auto_project_group = ana.udf.get(nsc.AUTO_POOL_UDF)
        if auto_project_group:
            group_inputs = input_map.get(auto_project_group, default=[])
            group_inputs.append(ana)

    groups = []
    for udf, inputs in input_map.items():
        input_ids = [i.id for i in inputs]
        if all(limsid in input_ids for limsid in udf.split(",")):
            groups.append(inputs)

    return inputs


def get_input_flowcell(queue):
    ''' Get a list of inputs from the queue of a step, 
    corresponding to all the pools on a  flow cell. If not all inputs are 
    present in the queue, the flow cell is ignored for now.
    
    If there are pools on the flow
    cell which do not have the automation flag set, these inputs are 
    ignored, and the flow cell may still be returned.
    '''

    flowcell_inputs = {} # FCID : input list

    for ana in queue:
        fcid = ana.location[0].id
        input_list = flowcell_inputs.get(fcid, default=[])
        input_list.append(ana)


    flowcell_groups = []
    for fcid,inputs in flowcell_inputs.items():
        fc = inputs.location[0]
        input_ids = [i.id for i in inputs]
        valid = True
        for art in fc.placements.values():
            if art.udf.get(nsc.AUTO_POOL_UDF):
                if art_id not in [input_ids]:
                    valid = False

        if valid:
            flowcell_groups.append(inputs)


    return flowcell_groups

        

def start_steps(lims, protocol_step, protocol_step_setup):
    '''Starts zero or more instances of a specific protocol step.
    
    protocol_step is a protocol step configuration entity from the LIMS.
    
    protocol_step_setup is a NSC-specific class which holds a few configuration 
    options.'''

    queue = protocol_step.queue
    if protocol_step_setup.grouping == "project":
        jobs = get_input_groups(queue.artifacts)
    elif protocol_step_setup.grouping == "flowcell":
        jobs = get_input_flowcell(queue.artifacts)
    else:
        raise Exception("Grouping not configured correctly for " + protocol_step_setup.name)
    
    steps = []
    for job in jobs:
        steps.append(lims.create_step(protocol_step, job))

    return steps



        

def start_automated_protocols(lims):
    '''Checks for samples in the automated protocols and starts steps if 
    possible, then executes a script on the "record details" screen if 
    configured.'''
    
    # Loop over protocols in config
    for protocol, protocol_steps in nsc.automated_protocol_steps:
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
                steps = start_steps(lims, ps, setup)
                
                # Run scripts if configured
                if setup.script:
                    for step in steps:
                        for ap in step.available_programs:
                            if ap.name == setup.script:
                                ap.trigger()


