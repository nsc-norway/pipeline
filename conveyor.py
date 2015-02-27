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




def automate(instrument, process):
    '''Tag a flow cell for automatic processing'''

    # Is the sequencing finished?
    if process.udf.get("Finish Date"):

        # Mark the pools for automatic processing (all sequencer types)
        mark_project_pools(process.all_inputs())

        # Finish the sequencing step
        finish_process(process)

        # Automated processing now triggered, can remove flag so we don't 
        # have to process it again.
        process.udf[nsc.AUTO_FLAG_UDF] = False
        process.put()



def check_new_processes(lims): 
    '''Query the API for new sequencing processes with automation flag'''
    for instr, process in nsc.SEQ_PROCESSES:
        ps = lims.get_processes(type = process, udf = {nsc.AUTO_FLAG_UDF: "on"})
        for p in ps:
            automate(instr, p)





def complete_hiseq(lims, process_id):
    process = Process(lims, id = process_id)

    if len(process.input_output_maps) == 0:
        print "There are no samples to process."
    else:
        # Input_output_maps returns a list of tuples with (input,output)
        # Get the input analytes to the sequencing process, which will also be the input to 
        # the following processes. There is one sample per lane.
        analytes = dict((x[0]['limsid'],x[0]['uri']) for x in process.input_output_maps)

        # For any item in the dictionary (pools), get the location, which is a tuple
        # of container and position, and save the container
        flowcell = analytes.itervalues().next().location[0]
        
        # Get the sample sheet. The sample sheet is an artifact of the Cluster 
        # Generation protocol step, which happened before the Sequencing step.
        # Get the parent process of any of the input pools
        fc_prepare_process = analytes.itervalues().next().parent
        

    # add it to the HiSeq data processing protocol step
    
    data = ()



