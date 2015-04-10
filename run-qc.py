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


def main(threads, run_dir, no_sample_sheet):
    run_id = os.path.basename(os.path.realpath(run_dir))
    match = re.match("^\d{6}_(NS|M)[A-Z0-9]+_\d{4}_[A-Z0-9\-]+$", run_id)
    if not match:
        print "Error: Specified directory doesn't look like a MiSeq or NextSeq run directory"
        sys.exit(1)

    if match.group(1) == "M":
        instrument = "miseq"
        # MiSeq has the Data/reports info, like HiSeq, getting clu. density from files
        pf_path = os.path.join(run_dir, "Data", "reports", "NumClusters By Lane PF.txt")
        lane_pf = get_lane_cluster_density(pf_path)
        raw_path = os.path.join(run_dir, "Data", "reports", "NumClusters By Lane.txt")
        lane_raw = get_lane_cluster_density(raw_path)
        lane = Lane(1, lane_raw[1], lane_pf[1], lane_pf[1] / lane_raw[1])

    elif match.group(1) == "NS":
        run_completion = ElementTree.parse(os.path.join(run_dir, "RunCompletionStatus.xml")).getroot()
        clus_den = float(run_completion.find("ClusterDensity").text)
        pf_ratio = float(run_completion.find("ClustersPassingFilter").text) / 100.0
        instrument = "nextseq"
        lane = Lane(1, clus_den, clus_den * pf_ratio, pf_ratio)

    demultiplex_dir = os.path.join(run_dir, "Data", "Intensities", "BaseCalls")
    if no_sample_sheet:
        info, projects = parse.get_ne_mi_seq_from_files(run_dir, lane)
    else:
        info, projects = parse.get_ne_mi_seq_from_ssheet(run_id, run_dir, instrument, lane)

    qc.qc_main(demultiplex_dir, projects, instrument, run_id, info['sw_versions'], threads)


def main_lims(threads, process_id):
    '''LIMS-based QC wrapper. 
    
    To be run in slurm job, called via epp-submit-slurm.py.'''

    process = Process(nsc.lims, id=process_id)
    utilities.running(process)
    seq_process = utilities.get_sequencing_process(process)

    run_id = seq_process.udf['Run ID']

    instrument = utilities.get_instrument(seq_process)
    if instrument == "miseq":
        demultiplex_dir = "Don't know yet..." 
    elif instrument == "nextseq":
        demux_process = utilities.get_demux_process(process)
        demultiplex_dir = demux_process.udf[nsc.DEST_FASTQ_DIR_UDF]
    else:
        raise ValueError("This script can only handle MiSeq and NextSeq")

    # Run directory on secondary storage
    dd = os.path.realpath(demultiplex_dir)
    m = re.match("(.*)/Data/Intensities/BaseCalls$", dd)
    if m:
        run_dir = m.group(1)
    else:
        raise RuntimeError("Directory structure doesn't match expectations")


    # Lane cluster density -- UDFs are set by Illumina Sequencing process for 
    # Mi and NextSeq ( to check this )
    lane = next(process.all_inputs())
    density_raw = lane.udf['Cluster Density (K/mm^2) R1']
    n_raw = lane.udf['Clusters Raw R1']
    n_pf = lane.udf['Clusters PF R1']
    density_pf = density_raw * n_pf / n_raw
    pf_ratio = lane.udf['%PF R1'] / 100.0
    # Using 1 logical lane even for NS until we can extract data from each lane
    # independently
    lane = parse.Lane(1, density_raw, density_pf, pf_ratio)

    info, projects = parse.get_ne_mi_seq_from_ssheet(run_id, run_dir, instrument, lane)
    qc.qc_main(demultiplex_dir, projects, instrument, run_id, info['sw_versions'], threads)
    utilities.success_finish(process)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', type=int, default=None, help='Number of threads (cores)')
    parser.add_argument('--pid', default=None, help="Process-ID if running within LIMS")
    parser.add_argument('--no-sample-sheet', action='store_true', help="Run without sample sheet, look for files")
    parser.add_argument('DIR', default=None, help="Run directory")
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
        main(threads, args.DIR, args.no_sample_sheet)
    else:
        print "Must specify either LIMS-ID of QC process or Unaligned (bcl2fastq output) directory"
        sys.exit(1)


