import sys, os.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from genologics.lims import *
from library import nsc, demultiplex

demultiplex.populate_results(Process(nsc.lims, id = sys.argv[1]), sys.argv[2])

