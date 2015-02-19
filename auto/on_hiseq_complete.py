# Script which runs when sequencing is complete and moves the run on to a
# bioinformatics protocol step, if applicable

import sys
from genologics.lims import *
import nsc


def process_process(lims, process_id):
  process = lims.Process(id = process_id)
  
  





# Script startup code
parser = ArgumentParser()
parser.add_argument("--log",default=sys.stdout, help="Log file")
parser.add_argument("--pid", help="Process id")

args = parser.parse_args()

with EppLogger(args.log):
  nsc.lims.check_version()
  process_process(nsc.lims, args.pid)

