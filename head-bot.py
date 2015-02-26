#------------------------------#
# Head Bot
# NSC data processing manager
#------------------------------#

# This script monitors the LIMS via the REST API and schedules processing 
# steps. Its main purpose is to start other jobs. It only updates a small 
# number of user defined fields (UDFs) in the API to manage the processing 
# status. Other information is set directly by the scripts called by the 
# head bot.

# This system doesn't have any storage system of its own -- all runtime
# data are kept in the LIMS.

import sys
import requests
from genologics.lims import *
from argparse import ArgumentParser
#from genologics.epp import EppLogger
import nsc



def check_new_processes(): 
    pass



def check



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


# Script startup code
parser = ArgumentParser()
parser.add_argument("--pid", help="Process id")

args = parser.parse_args()

nsc.lims.check_version()
complete_hiseq(nsc.lims, args.pid)


