import ..demultiplex
from genologics.lims import *
import nsc
import sys

demultiplex.populate_results(Process(nsc.lims, id = sys.argv[1]), sys.argv[2])

