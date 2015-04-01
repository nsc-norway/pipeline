# Quality control script

# This is a manual interface to the QC library. No LIMS interaction is 
# required, but the modules must be in place, as we haven't enforced 
# a strict separation.

import re
import sys, os
import argparse, glob
from collections import defaultdict
from genologics import *
import nsc, utilities
import qc
import parse

def get_project_sample():
    pass

def qc_hiseq():
    pass


def get_lane_cluster_density(path):
    '''Get cluster density for lanes from report files.
    
    Returns a dict indexed by the 1-based lane number.'''

    with open(path) as f:
        # Discard up to and including Lane
        while not next(f).startswith("Lane\t"):
            pass

        lane_sum = defaultdict(int)
        lane_ntile = defaultdict(int)
        for l in f:
            cell = l.split("\t")
            lane_sum[int(cell[0])] += float(cell[2])
            lane_ntile[int(cell[0])] += 1

        return dict((i+1, lane_sum[i] / lane_ntile[i]) for i in lane_sum.keys())



def main(threads, demultiplex_dir):
    run_dir = os.path.join(demultiplex_dir, "..")
    run_id = os.path.basename(os.path.realpath(run_dir))
    if not re.match("^\d{6}_[A-Z0-9]+_\d{4}_[A-Z0-9]+$", run_id):
        print "Error: Parent of specified directory doesn't look like a run directory"
        sys.exit(1)

    # Trying to stay in demultiplex_dir as much as possible, but this data
    # can only be had from the global run data dir in a form that's not too
    # error-prone.
    pf_path = os.path.join(run_dir, "Data", "reports", "NumClusters By Lane PF.txt")
    lane_pf = get_lane_cluster_density(pf_path)
    raw_path = os.path.join(run_dir, "Data", "reports", "NumClusters By Lane.txt")
    lane_raw = get_lane_cluster_density(raw_path)

    dm_path = glob.glob(os.path.join(demultiplex_dir, "Basecall_Stats_*", "Flowcell_demux_summary.xml"))[0]
    demux_summary = parse.parse_demux_summary(dm_path)
    n_reads = len(demux_summary[0].values()[0])
    print "Number of non-index reads:", n_reads
    
    lanes = {}
    for l in lane_raw.keys():
        lanes[l] = parse.Lane(l, lane_raw[l], lane_pf[l], lane_pf[l] / lane_raw[l])

    print "Number of lanes to process:", len(lanes)

    info, projects = parse.get_hiseq_qc_data(run_id, n_reads, lanes, demultiplex_dir)
    qc.qc_main(demultiplex_dir, projects, run_id, info['sw_versions'], threads)


def main_lims():
    try:
        pass
    except:
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', type=int, default=2, help='Number of threads (cores)')
    parser.add_argument('--pid', default=None, help="Process-ID if running within LIMS")
    parser.add_argument('DIR', default=None, help="Demultiplexed data directory (Unaligned for HiSeq, BaseCalls for Mi/NextSeq)")
    args = parser.parse_args()
    if args.pid and not args.DIR:
        main_lims(args.threads, args.pid)
    elif args.DIR and not args.pid:
        main(args.threads, args.DIR)
    else:
        print "Must specify either LIMS-ID of project or Unaligned (bcl2fastq output) directory"
        sys.exit(1)


