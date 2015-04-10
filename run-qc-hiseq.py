#!/bin/env python
# Quality control script

# This is a manual interface to the QC library. No LIMS interaction is 
# required, but the modules must be in place, as we haven't enforced 
# a strict separation.

import re
import sys, os
import argparse, glob
from collections import defaultdict
from genologics import *
from common import nsc, utilities, qc, parse


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
    # Number of reads: take the first sample and lane, count the reads
    n_reads = len(demux_summary[0].values()[0].values()[0])
    print "Number of non-index reads:", n_reads
    
    lanes = {}
    for l in lane_raw.keys():
        lanes[l] = parse.Lane(l, lane_raw[l], lane_pf[l], lane_pf[l] / lane_raw[l])

    print "Number of lanes to process:", len(lanes)

    info, projects = parse.get_hiseq_qc_data(run_id, n_reads, lanes, demultiplex_dir)
    qc.qc_main(demultiplex_dir, projects, 'hiseq', run_id, info['sw_versions'], threads)


def main_lims(threads, process_id):
    '''LIMS-based QC wrapper. 
    
    To be run in slurm job, called via epp-submit-slurm.py.'''

    process = Process(nsc.lims, id=process_id)

    utilities.running(process)

    seq_process = utilities.get_sequencing_process(process)
    demux_process = utilities.get_demux_process(process)

    run_id = seq_process.udf['Run ID']
    for n_reads in xrange(10):
        try:
            rc = seq_process.udf["Read {0} Cycles".format(r)]
        except KeyError:
            break

    demultiplex_dir = demux_process.udf[nsc.DEST_FASTQ_DIR_UDF]
    
    lanes = {}
    for lane in process.all_inputs():
        lane_id = int(re.match("(\d+):1", lane.location[1]).group(1))
        # UDFs are set by Illumina Sequencing process
        density_raw = lane.udf['Cluster Density (K/mm^2) R1']
        n_raw = lane.udf['Clusters Raw R1']
        n_pf = lane.udf['Clusters PF R1']
        density_pf = density_raw * n_pf / n_raw
        pf_ratio = lane.udf['%PF R1'] / 100.0
        lanes[l] = parse.Lane(lane_id, density_raw, density_pf, pf_ratio)

    info, projects = parse.get_hiseq_qc_data(run_id, n_reads, lanes, demultiplex_dir)
    qc.qc_main(demultiplex_dir, projects, 'hiseq', run_id, info['sw_versions'], threads)

    utilities.success_finish(process)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', type=int, default=None, help='Number of threads (cores)')
    parser.add_argument('--pid', default=None, help="Process-ID if running within LIMS")
    parser.add_argument('DIR', default=None, help="Demultiplexed data directory (Unaligned)")
    args = parser.parse_args()
    threads = args.threads
    if not threads:
        try:
            threads = int(os.environ['SLURM_CPUS_ON_NODE'])
            print "Threads from slurm: ", threads
        except KeyError:
            threads = 1

    if args.pid and not args.DIR:
        try:
            main_lims(threads, args.pid)
        except:
            process = Process(nsc.lims, id=args.pid)
            utilities.fail(process, "Unexpected: " + str(sys.exc_info()[1]))
            raise
    elif args.DIR and not args.pid:
        main(threads, args.DIR)
    else:
        print "Must specify either LIMS-ID of QC process or Unaligned (bcl2fastq output) directory"
        sys.exit(1)


