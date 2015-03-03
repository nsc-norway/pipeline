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



def init_automation(instrument, process):
    '''This should be called on sequencing processes which already have
    the automation flag set. This clears the process-level automation flag 
    and sets UDFs on the analytes (pools) instead. It will only run if all
    QC flags are set.'''

    # Is the sequencing finished?
    if process.udf.get("Finish Date"):

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


def get_input_jobs(queue, grouping):
    

    # Grouped by UDF
    input_map = {}
    for ana in queue.artifacts:
        auto_project_group = ana.udf.get(nsc.AUTO_POOL_UDF)
        if auto_project_group:
            input_map.get()

    


def start_automated_protocols(lims):
    '''Checks for samples in the automated protocols and starts steps if 
    possible.'''
    
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
                q = ps.queue()
                jobs = get_input_groups(q, step.grouping)

                for job in jobs:
                    step = lims.create_step(ps, job)
                    step.execute_script() # todo

