#!/bin/env python
# Quality control script

# This script has both a manual interface and a LIMS interface to the QC
# module. No LIMS interaction is required, but the modules must be in place,
# as we haven't enforced a strict separation.


# SBATCH HEADERS FOR NON-LIMS SLURM OPERATION
# (for lims it uses the wrapper script in slurm/)
#SBATCH --account=nsc
#SBATCH --qos=high
#SBATCH --partition=main
#SBATCH --time=1-0

# Job resources.
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=4G

# Set performance options:
#SBATCH --mem_bind=local
#SBATCH --hint=compute_bound
#SBATCH --hint=multithread
# END SBATCH OPTIONS


import re
import sys, os
import argparse, glob
from collections import defaultdict
from xml.etree import ElementTree

if "--sbatch" in sys.argv:
    # Hacky way...
    sys.path.insert(0, "/data/nsc.loki/automation/pipeline")

from genologics.lims import *
from common import nsc, utilities, qc, parse



def main(threads, run_dir, no_sample_sheet, process_undetermined):
    run_id = os.path.basename(os.path.realpath(run_dir))
    match = re.match(r"^\d{6}_(NS|M)[A-Z0-9]+_\d{4}_[A-Z0-9\-]+$", run_id)
    if not match:
        print "Error: Specified directory doesn't look like a MiSeq or NextSeq run directory"
        sys.exit(1)

    if match.group(1) == "M":
        instrument = "miseq"
        # MiSeq has the Data/reports info, like HiSeq, getting clu. density from files
        pf_path = os.path.join(run_dir, "Data", "reports", "NumClusters By Lane PF.txt")
        lane_pf = parse.get_lane_cluster_density(pf_path)
        raw_path = os.path.join(run_dir, "Data", "reports", "NumClusters By Lane.txt")
        lane_raw = parse.get_lane_cluster_density(raw_path)
        lanes = [qc.Lane(1, lane_raw[1], lane_pf[1], lane_pf[1] / lane_raw[1])]
        use_merged_lanes = False

    elif match.group(1) == "NS":
        instrument = "nextseq"
        run_completion = ElementTree.parse(
                os.path.join(run_dir, "RunCompletionStatus.xml")).getroot()
        clus_den = float(run_completion.find("ClusterDensity").text)
        pf_ratio = float(run_completion.find("ClustersPassingFilter").text) / 100.0
        #lanes = [qc.Lane(l, clus_den, clus_den * pf_ratio, pf_ratio) for l in (1,2,3,4)]
        # For merged files:
        lanes = [qc.Lane("X", clus_den, clus_den * pf_ratio, pf_ratio)]
        use_merged_lanes = True


    demultiplex_dir = os.path.join(run_dir, "Data", "Intensities", "BaseCalls")
    info, projects = get_ne_mi_seq_from_ssheet(run_id, run_dir, instrument, lanes, use_merged_lanes,
            include_undetermined=process_undetermined)

    qc.qc_main(demultiplex_dir, projects, instrument, run_id, info['sw_versions'], threads)


def main_lims(threads, process_id):
    """LIMS-based QC wrapper. 
    
    To be run in slurm job, called via epp-submit-slurm.py."""

    process = Process(nsc.lims, id=process_id)
    utilities.running(process)
    seq_process = utilities.get_sequencing_process(process)

    run_id = seq_process.udf['Run ID']

    instrument = utilities.get_instrument(seq_process)
    if instrument == "miseq":
        demultiplex_dir = os.path.join(nsc.SECONDARY_STORAGE, run_id, "Data", "Intensities", "BaseCalls")
    elif instrument == "nextseq":
        demux_process = utilities.get_demux_process(process)
        demultiplex_dir = demux_process.udf[nsc.DEST_FASTQ_DIR_UDF]
    else:
        raise ValueError("This script can only handle MiSeq and NextSeq runs")

    # Run directory on secondary storage
    dd = os.path.realpath(demultiplex_dir)
    m = re.match("(.*)/Data/Intensities/BaseCalls$", dd)
    if m:
        run_dir = m.group(1)
    else:
        raise RuntimeError("Directory structure doesn't match expectations")


    # Lane cluster density -- UDFs are set by Illumina Sequencing process for 
    # Mi and NextSeq ( to check this )
    lane = process.all_inputs()[0]
    density_raw = lane.udf['Cluster Density (K/mm^2) R1']
    n_raw = lane.udf['Clusters Raw R1']
    n_pf = lane.udf['Clusters PF R1']
    density_pf = density_raw * n_pf / n_raw
    pf_ratio = lane.udf['%PF R1'] / 100.0
    if instrument == "miseq":
        lanes = [qc.Lane(1, density_raw * 1000.0, density_pf * 1000.0, pf_ratio)]
        use_merged_lanes = False
    else:
        #lanes = [qc.Lane(l, density_raw * 1000.0, density_pf * 1000.0, pf_ratio) for l in (1,2,3,4)]
        # Merged lane fastq files
        lanes = [qc.Lane("X", density_raw * 1000.0, density_pf * 1000.0, pf_ratio)]
        use_merged_lanes = True

    info, projects = get_ne_mi_seq_from_ssheet(run_id, run_dir, instrument, lanes, use_merged_lanes,
            include_undetermined=process.udf[nsc.PROCESS_UNDETERMINED_UDF])
    qc.qc_main(demultiplex_dir, projects, instrument, run_id, info['sw_versions'], threads)
    utilities.success_finish(process)


def get_sw_versions(run_dir):
    """Mainly for Mi/NextSeq"""

    try:
        xmltree = ElementTree.parse(os.path.join(run_dir, 'RunParameters.xml'))
    except IOError:
        xmltree = ElementTree.parse(os.path.join(run_dir, 'runParameters.xml'))

    run_parameters = xmltree.getroot()
    rta_ver = run_parameters.find("RTAVersion").text
    return [("RTA", rta_ver)]


def get_ne_mi_seq_from_ssheet(run_id, run_dir, instrument, lanes, merged_lanes=False,
        sample_sheet_path=None, include_undetermined=False):
    """Get NextSeq or MiSeq QC model objects.

    Gets the info from the sample sheet and demultiplexing stats. Works for
    indexed projects and for projects with no index, but with an entry in the
    sample sheet.
    
    This is limited compared to the HiSeq version -- many fields are filled
    with None because they are not available / we don't use them."""

    if not sample_sheet_path:
        if instrument == "miseq":
            sample_sheet_path = os.path.join(run_dir, "Data", "Intensities", "BaseCalls", 'SampleSheet.csv')
        elif instrument == "nextseq":
            sample_sheet_path = os.path.join(run_dir, 'SampleSheet.csv')

    sample_sheet = parse.parse_ne_mi_seq_sample_sheet(open(sample_sheet_path).read())
    n_reads = len(sample_sheet['reads'])
    try:
        project_name = sample_sheet['data'][0]['project']
    except KeyError: 
        project_name = sample_sheet['header']['Experiment Name']

    project_dir = parse.get_project_dir(run_id, project_name)

    if instrument == "miseq":
        stats = {}
    elif instrument == "nextseq":
        stats = parse.get_nextseq_stats(
                os.path.join(run_dir, "Data", "Intensities", "BaseCalls", "Stats"),
                aggregate_lanes=True
                )

    samples = []
    for sam_index, sam in enumerate(sample_sheet['data']):
        files = []
        sample_name = sam['samplename']
        if not sample_name:
            sample_name = sam['sampleid']
        for lane in lanes:
            for ir in xrange(1, n_reads+1):
                if merged_lanes:
                    path = "{0}/{1}_S{2}_R{3}_001.fastq.gz".format(
                            project_dir, sample_name, str(sam_index + 1), ir
                            )
                else:
                    path = "{0}/{1}_S{2}_L{3}_R{4}_001.fastq.gz".format(
                            project_dir, sample_name, str(sam_index + 1),
                            str(lane.id).zfill(3), ir)
                file_stats = stats.get((lane.id, sample_name, ir))
                files.append(qc.FastqFile(lane, ir, path, file_stats))
                print "file ", path

        sample = qc.Sample(sample_name, files)
        samples.append(sample)

    project = qc.Project(project_name, project_dir, samples)

    if include_undetermined:
        if merged_lanes:
            unfiles = [
                    qc.FastqFile(
                        lane, ir, "Undetermined_S0_R%d_001.fastq.gz" % (ir),
                        stats.get((lane.id, None, ir))
                        )
                    for ir in xrange(1, n_reads + 1) for lane in lanes
                    ]
        else:
            unfiles = [
                    qc.FastqFile(
                        lane, ir, "Undetermined_S0_L%s_R%d_001.fastq.gz" % (str(lane.id).zfill(3), ir),
                        stats.get((lane.id, None, ir))
                        )
                    for ir in xrange(1, n_reads + 1) for lane in lanes
                    ]
        unsample = qc.Sample("Undetermined", unfiles)
        unproject = qc.Project("Undetermined_indices", None, [unsample], is_undetermined=True)
        projects = [unproject, project]
    else:
        projects = [project]

    info = {'sw_versions': get_sw_versions(run_dir)}
    return info, projects
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sbatch', default=False, action='store_true', help="Running under sbatch (not working well)")
    parser.add_argument('--threads', type=int, default=None, help='Number of threads (cores)')
    parser.add_argument('--pid', default=None, help="Process-ID if running within LIMS")
    parser.add_argument('--no-sample-sheet', action='store_true', help="Run without sample sheet, look for files")
    parser.add_argument('--process-undetermined', action='store_true', help="Process undetermined indexes")
    parser.add_argument('DIR', default=None, nargs='?', help="Run directory")
    args = parser.parse_args()
    threads = args.threads
    try:
        threads = int(os.environ['SLURM_CPUS_ON_NODE'])
        print "Threads from slurm: ", threads
    except KeyError:
        pass

    if args.pid and not args.DIR:
        with utilities.error_reporter(args.pid):
            main_lims(threads, args.pid)

    elif args.DIR and not args.pid:
        main(threads, args.DIR, args.no_sample_sheet, args.process_undetermined)
    else:
        print "Must specify either LIMS-ID of QC process or bcl2fastq2 output directory"
        sys.exit(1)


