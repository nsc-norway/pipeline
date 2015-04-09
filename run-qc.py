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
    elif match.group(1) == "NS":
        instrument = "nextseq"
    else:
        raise ValueError("The given directory is not a MiSeq or NextSeq run.")

    demultiplex_dir = os.path.join(run_dir, "Data", "Intensities", "BaseCalls")
    if no_sample_sheet:
        info, projects = parse.get_ne_mi_seq_from_files(run_dir)
    else:
        info, projects = parse.get_ne_mi_seq_from_ssheet(run_id, run_dir, instrument)

    qc.qc_main(demultiplex_dir, projects, instrument, run_id, info['sw_versions'], threads)


def main_lims(threads, process_id):
    '''LIMS-based QC wrapper. 
    
    To be run in slurm job, called via epp-submit-slurm.py.'''

    # TODO!! this is the HiSeq version....
    process = Process(nsc.lims, id=process_id)
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
    qc.qc_main(demultiplex_dir, projects, instrument, run_id, info['sw_versions'], threads)


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


