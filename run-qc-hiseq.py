#!/bin/env python
# Quality control script

# This is a manual interface to the QC library. No LIMS interaction is 
# required, but the modules must be in place, as we haven't enforced 
# a strict separation.

import re
import sys, os
import argparse, glob
from collections import defaultdict
from xml.etree import ElementTree
from genologics.lims import *
from common import nsc, utilities, qc, parse



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
    lane_pf = parse.get_lane_cluster_density(pf_path)
    raw_path = os.path.join(run_dir, "Data", "reports", "NumClusters By Lane.txt")
    lane_raw = parse.get_lane_cluster_density(raw_path)
    lanes = {}
    for l in lane_raw.keys():
        lanes[l] = qc.Lane(l, lane_raw[l], lane_pf[l], lane_pf[l] / lane_raw[l])
    print "Number of lanes to process:", len(lanes)

    # Parse demux summary just to get the number of reads
    dm_path = glob.glob(os.path.join(
        demultiplex_dir, "Basecall_Stats_*", "Flowcell_demux_summary.xml"
        ))[0]
    demux_summary = parse.parse_demux_summary(dm_path, False)
    # Number of reads: take the first sample and lane, count the reads (kind of a
    # lame way to do it, but it avoids the need for another file parser)
    n_reads = max(k[2] for k in demux_summary.keys())
    print "Number of non-index reads:", n_reads
    print "Including undetermined indexes for QC analysis"

    info, projects = get_hiseq_qc_data(run_id, n_reads, lanes, demultiplex_dir, True)
    qc.qc_main(demultiplex_dir, projects, 'hiseq', run_id, info['sw_versions'], threads)


def main_lims(threads, process_id):
    """LIMS-based QC wrapper. 
    
    To be run in slurm job, called via epp-submit-slurm.py."""

    process = Process(nsc.lims, id=process_id)

    utilities.running(process)

    seq_process = utilities.get_sequencing_process(process)
    demux_process = utilities.get_demux_process(process)

    run_id = seq_process.udf['Run ID']
    for ir in xrange(1, 10):
        try:
            if seq_process.udf["Read {0} Cycles".format(ir)]:
                n_reads = ir
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
        lanes[lane_id] = qc.Lane(lane_id, density_raw * 1000.0, density_pf * 1000.0, pf_ratio)

    info, projects = get_hiseq_qc_data(run_id, n_reads, lanes, demultiplex_dir, process.udf[nsc.PROCESS_UNDETERMINED_UDF])
    qc.qc_main(demultiplex_dir, projects, 'hiseq', run_id, info['sw_versions'], threads)

    utilities.success_finish(process)


def get_hiseq_sw_versions(demultiplex_config):
    """Get a dict with software names->versions.
    
    demultiplex_config is an Element object (from ElementTree)"""

    sw_versions = []
    # Software tags are nested (best way to see is to just look at the xml)
    sw_tags = [demultiplex_config.find('Software')]
    while sw_tags:
        tag = sw_tags.pop()
        sw_tags += tag.findall("Software")
        if tag.attrib['Name'] == "configureBclToFastq.pl": #special case
            name, ver = tag.attrib['Version'].split('-')
            sw_versions.append((name, ver))
        # Add RTA just once
        elif "RTA" in tag.attrib['Name'] and "RTA" not in [sv[0] for sv in sw_versions]:
            sw_versions.append((tag.attrib['Name'], tag.attrib['Version']))

    return sw_versions


def get_hiseq_qc_data(run_id, n_reads, lanes, root_dir, include_undetermined = True):
    """Get HiSeq metadata about project, sample and files, including QC data. 
    Converted to the internal representation (model) classes defined above.

    n_reads is the number of sequence read passes, 1 or 2 (paired end)

    lanes is a dict with key: numeric lane number, value: lane object
    """
    
    # Getting software and sample information from DemultiplexConfig.xml
    # It has almost exactly the same data as the sample sheet, but it has the
    # advantage that it's always written by bcl2fastq, so we know that we're getting
    # the one that was used for demultiplexing.
    xmltree = ElementTree.parse(os.path.join(root_dir, "DemultiplexConfig.xml"))
    demultiplex_config = xmltree.getroot()
    sw_versions = get_hiseq_sw_versions(demultiplex_config)

    flowcell_info = demultiplex_config.find("FlowcellInfo")
    fcid = flowcell_info.attrib['ID']

    # List of sample x lane
    entries = []
    for lane in flowcell_info.findall("Lane"):
        for sample in lane.findall("Sample"):
            sd = dict(sample.attrib)
            sd['Lane'] = lane.attrib['Number']
            entries.append(sd)

    # Project -> [Sample x lane]
    project_entries = defaultdict(list)
    for sample_entry in entries:
        project_entries[sample_entry['ProjectId']].append(sample_entry)

    # Getting stats from Flowcell_demux_summary.xml (no longer using Demultiplex_stats.htm).
    ds_path = os.path.join(root_dir, "Basecall_Stats_" + fcid, "Flowcell_demux_summary.xml")
    demux_sum = parse.get_hiseq_stats(ds_path)

    projects = []
    for proj, entries in project_entries.items():
        undetermined = re.match("Undetermined_indices$", proj)
        if undetermined:
            if not include_undetermined:
                continue

            project_dir = "Undetermined_indices"
        else:
            project_dir = parse.get_hiseq_project_dir(run_id, proj)

        samples = {}
        for e in entries:
            sample_dir = project_dir + "/Sample_" + e['SampleId']
            files = []
            for ri in xrange(1, n_reads + 1):
                # Empty files will not have any stats, that's why we use get(), not []
                stats = demux_sum.get((int(e['Lane']), e['SampleId'], ri))

                # FastqFile
                path_t = sample_dir + "/{0}_{1}_L{2}_R{3}_001.fastq.gz"
                path = path_t.format(e['SampleId'], e['Index'], e['Lane'].zfill(3), ri)
                lane = lanes[int(e['Lane'])]
                f = qc.FastqFile(lane, ri, path, stats)
                files.append(f)

            sample = samples.get(e['SampleId'])
            if not sample:
                sample = qc.Sample(e['SampleId'], [])
                samples[e['SampleId']] = sample

            sample.files += files

        # Project 
        p = qc.Project(proj, project_dir, samples.values(), is_undetermined=undetermined)
        projects.append(p)

    info = {"sw_versions": sw_versions}

    return info, projects





if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', type=int, default=None, help='Number of threads (cores)')
    parser.add_argument('--pid', default=None, help="Process-ID if running within LIMS")
    parser.add_argument('DIR', nargs='?', default=None, help="Demultiplexed data directory (Unaligned)")
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


