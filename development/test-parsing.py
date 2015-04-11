import sys, os.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from genologics.lims import *
from common import nsc, parse

stats = parse.get_nextseq_stats("../../ns")
print stats[("unknown", 1)]


