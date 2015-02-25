# Script which runs when sequencing is complete.

# This script moves the sample on to the automatic demux / QC workflow.

import sys
from genologics.lims import *
from argparse import ArgumentParser
#from genologics.epp import EppLogger
import nsc




def process_process(lims, process_id):
    process = Process(lims, id = process_id)
    # input_output_maps returns a list of tuples with (input,output)
    analytes = dict((x[0]['limsid'],x[0]['uri']) for x in process.input_output_maps)
    
    # add it to the HiSeq data processing protocol step
    
    data = ()


# Script startup code
parser = ArgumentParser()
parser.add_argument("--pid", help="Process id")

args = parser.parse_args()

nsc.lims.check_version()
process_process(nsc.lims, args.pid)

