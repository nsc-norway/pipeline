# Parsing of bcl2fastq2 output, and Illumina Analysis Software output for the MiSeq

import re
import os
import itertools
import utilities
import samples
from xml.etree import ElementTree
from collections import defaultdict
from Counter import Counter


###################### BCL2FASTQ2 STATS #######################

def parse_conversion_stats(conversion_stats_path, aggregate_lanes, aggregate_reads):
    """Get "conversion stats" from the bcl2fastq2 stats.

    Loops over the comprehensive tree structure of the ConversionStats.xml
    file and generates the grand totals. 

    Data are returned in a dict indexed by sample name and read number (1/2). Stats
    which are the same for both reads are replicated in the values for both reads.

    {
        (coordinates) => ( {sample stats}, {sample stats PF} )
    }

    Depending on the aggregate_X parameters, the coordinates can be:
     ("X", project, sample name, 1)
     (lane, project, sample name, 1)
     (lane, project, sample name, read)

    Undetermined indexes are reported as project name = None, sample name = None.
    """
    xmltree = ElementTree.parse(conversion_stats_path)
    root = xmltree.getroot()
    if root.tag != "Stats":
        raise RuntimeError("Expected XML element Stats, found " + root.tag)
    fc = root.find("Flowcell")

    projects = []
    default_project = None
    for pro in fc.findall("Project"):
        if pro.attrib['name'] == "default":
            default_project = pro
        elif pro.attrib['name'] != "all":
            projects.append(pro)

    
    # We compute the most specific coordinate first, and then take the 
    # aggregates (sum) later.
    samples = {}

    # Which projects to process
    if default_project:
        analyse_projects = [(True, default_project)] + [(False, project) for project in projects]
    else:
        analyse_projects = [(False, project) for project in projects]

    for is_undetermined, project in analyse_projects:
        for sample in project.findall("Sample"):
            if is_undetermined:
                sample_id = None
                if not (sample.attrib['name'] == "unknown" or
                        sample.attrib['name'] == "Undetermined"):
                    continue
                else:
                    project_name = None
            else:
                sample_id = sample.attrib['name']
                if sample_id == "all":
                    continue
                project_name = project.attrib['name']


            # stats_* are indexed by filter type (pf/raw) then by the read index (then will
            # be re-organised in the return value)
            barcode = next(bar for bar in sample.findall("Barcode") if bar.attrib['name'] == 'all')
            for lane in barcode.findall("Lane"):
                stats_pf = defaultdict(int)
                stats_raw = defaultdict(int)
                read_stats_pf = {}
                read_stats_raw = {}
                for tile in lane.findall("Tile"):
                    for filtertype in tile:
                        ft = filtertype.tag
                        if ft == "Raw":
                            st = stats_raw
                            rst = read_stats_raw
                        elif ft == "Pf":
                            st = stats_pf
                            rst = read_stats_pf
                        for read_or_cc in filtertype:

                            if read_or_cc.tag == "ClusterCount":
                                st["ClusterCount"] += int(read_or_cc.text)

                            elif read_or_cc.tag == "Read":
                                iread = int(read_or_cc.attrib['number'])
                                if iread > 3 or iread <= 0:     # Hack for samples without barcode: gets
                                                                # read == some random number
                                    iread = 1
                                if not rst.has_key(iread):
                                    rst[iread] = defaultdict(int)

                                for stat in read_or_cc:
                                    rst[iread][stat.tag] += int(stat.text)

                lane_id = int(lane.attrib['number'])
                for iread in read_stats_pf.keys():
                    # Include the read-independent stats into per-read stats
                    read_stats_pf[iread].update(stats_pf)
                    read_stats_raw[iread].update(stats_raw)
                    samples[(lane_id, project_name, sample_id, iread)] = (read_stats_raw[iread], read_stats_pf[iread])

    # sorry
    if aggregate_lanes:
        # Sort by key ([0], which is the coordinates) and skip first  [1:] (lane)
        sort_samples = sorted(samples.items(), key=lambda x: x[0][1:])
        samples = {}
        for key, group in itertools.groupby(sort_samples, key=lambda x: x[0][1:]):
            stats_raw = Counter()
            stats_pf = Counter()
            for key, (row_raw, row_pf) in group:
                stats_raw.update(row_raw)
                stats_pf.update(row_pf)
            samples[("X",) + key[1:]] = dict(stats_raw), dict(stats_pf)

    if aggregate_reads:
        # Sort by key ([0]) and skip last (read index), set read=1 for all
        sort_samples = sorted(samples.items(), key=lambda x: x[0][:-1])
        samples = {}
        for key, group in itertools.groupby(sort_samples, key=lambda x: x[0][:-1]):
            stats_raw = Counter()
            stats_pf = Counter()
            for key, (row_raw, row_pf) in group:
                stats_raw.update(row_raw)
                stats_pf.update(row_pf)
            samples[key[:-1] + (1,)] = dict(stats_raw), dict(stats_pf)

    return samples


def parse_demultiplexing_stats(conversion_stats_path, aggregate_lanes):
    """Sum up "demultiplexing stats" from the bcl2fastq2 stats directory.
    
    Contains information about barcode reads for runs demultiplexed by bcl2fastq2.
    The returned dict is indexed by the lane, project and sample name, and it also
    includes an entry for sample None, which corresponds to undetermined indexes.
    
    Returns: {
         (lane, project, sample name) => {stats}
    } 
    or {
         ("X", project, sample name) => {stats}
    } 
    """
    xmltree = ElementTree.parse(conversion_stats_path)
    root = xmltree.getroot()
    if root.tag != "Stats":
        raise RuntimeError("Expected XML element Stats, found " + root.tag)
    fc = root.find("Flowcell")

    projects = []
    default_project = None
    for pro in fc.findall("Project"):
        if pro.attrib['name'] == "default":
            default_project = pro
        elif pro.attrib['name'] != "all":
            projects.append(pro)

    # Real samples
    samples = {}
    for project in projects:
        for sample in project.findall("Sample"):
            sample_id = sample.attrib['name']
            if sample_id == "all":
                continue # Don't need "all"
            # only look at barcode="all"
            barcode = next(bar for bar in sample.findall("Barcode") if bar.attrib['name'] == 'all')
            if aggregate_lanes:
                stats = defaultdict(int)
                key = ("X", project.attrib['name'], sample_id)
            for lane in barcode.findall("Lane"):
                if not aggregate_lanes:
                    stats = defaultdict(int)
                    key = (int(lane.attrib['number']), project.attrib['name'], sample_id)

                for stat in lane:
                    stats[stat.tag] += int(stat.text)

                samples[key] = stats

    # Undetermined
    if default_project:
        sample = next(
                sam for sam in default_project.findall("Sample")
                if sam.attrib['name'] == "unknown" or sam.attrib['name'] == "Undetermined"
                )
        barcode = next(bar for bar in sample.findall("Barcode") if bar.attrib['name'] == 'all')
        stats = defaultdict(int)
        for lane in barcode.findall("Lane"):
            if aggregate_lanes:
                key = ("X", None, None)
            else:
                key = (int(lane.attrib['number']), None, None)
                stats = defaultdict(int)

            for stat in lane:
                stats[stat.tag] += int(stat.text)

            samples[key] = stats

    return samples


def get_bcl2fastq_stats(stats_xml_file_path, aggregate_lanes=True, aggregate_reads=False):
    """Function for the NextSeq, to compute the usual demultiplexing stats.

    Params: stats_xml_file_path: path to the directory containing the XML files
    (Data/Intensities/BaseCalls/Stats)

    aggregate_lanes:  add up stats for lanes (or average over, for percentages), and
                      only return coordinates with lane = "X"
    aggregate_reads:  add up reads, return read = 1 for all

    This function computes derived statistics based on the accumulated data 
    from the two above functions. 

    It returns a dict indexed by lane, project, sample name, and read, with the values
    being a dict indexed by the stats name:
    { (lane, project, sample_name, read) => {stat => value} }
    """
    
    demultiplexing_stats = parse_demultiplexing_stats(
            os.path.join(stats_xml_file_path, "DemultiplexingStats.xml"),
            aggregate_lanes
            )
    conversion_stats = parse_conversion_stats(
            os.path.join(stats_xml_file_path, "ConversionStats.xml"),
            aggregate_lanes, aggregate_reads
            )

    if aggregate_lanes:
        lanes = ("X",)
    else:
        lanes = sorted(set(c[0] for c in conversion_stats.keys()))


    # Totals: sum all sample clusters in each lane (read 1 only)
    all_raw_reads = dict(
            (lane, 
                sum(val[0]['ClusterCount'] 
                    for coord, val in conversion_stats.items()
                    if coord[0] == lane and coord[-1] == 1)
                )
            for lane in lanes
            )

    all_pf_reads = dict(
            (lane, 
                sum(val[1]['ClusterCount'] 
                    for coord, val in conversion_stats.items()
                    if coord[0] == lane and coord[-1] == 1)
                )
            for lane in lanes
            )

    result = {}
    for coordinates in conversion_stats.keys():
        lane, project, sample, read = coordinates
        de_s = demultiplexing_stats[(lane, project, sample)]
        con_s_raw, con_s_pf = conversion_stats[coordinates]

        stats = {}
        stats['# Reads'] = con_s_raw['ClusterCount']
        stats['# Reads PF'] = con_s_pf['ClusterCount']
        stats['Yield PF (Gb)'] = con_s_pf['Yield'] / 1e9
        if con_s_raw['ClusterCount'] > 0:
            stats['%PF'] = con_s_pf['ClusterCount'] * 100.0 / con_s_raw['ClusterCount']
        else:
            stats['%PF'] = 0.0
        if all_raw_reads[lane] != 0.0:
            stats['% of Raw Clusters Per Lane'] = con_s_raw['ClusterCount'] * 100.0 / all_raw_reads[lane]
        else:
            stats['% of Raw Clusters Per Lane'] = 0.0
        if all_pf_reads[lane] != 0.0:
            stats['% of PF Clusters Per Lane'] = con_s_pf['ClusterCount'] * 100.0 / all_pf_reads[lane]
        else:
            stats['% of PF Clusters Per Lane'] = 0.0
        if de_s['BarcodeCount'] != 0.0:
            stats['% Perfect Index Read'] = de_s['PerfectBarcodeCount'] * 100.0 / de_s['BarcodeCount']
            stats['% One Mismatch Reads (Index)'] = de_s['OneMismatchBarcodeCount'] * 100.0 / de_s['BarcodeCount']
        else:
            stats['% Perfect Index Read'] = 0
            stats['% One Mismatch Reads (Index)'] = 0
        if con_s_pf['Yield'] > 0:
            stats['% Bases >=Q30'] = con_s_pf['YieldQ30'] * 100.0 / con_s_pf['Yield']
            stats['Ave Q Score'] = con_s_pf['QualityScoreSum'] * 1.0 / con_s_pf['Yield']
        else:
            stats['% Bases >=Q30'] = 0.0
            stats['Ave Q Score'] = 0.0
        result[coordinates] = stats

    return result


###################### WRAPPER FNC #######################
def get_stats(
        instrument, 
        run_dir,
        aggregate_lanes=True,
        suffix=""
        ):
    """Instrument-independent interface to the stats module.

    We used to handle MiSeq on-instrument demultiplexing, but that 
    support is now dropped, since we use bcl2fastq2.
    """

    stats_xml_file_path = os.path.join(run_dir, "Data", "Intensities", "BaseCalls", "Stats" + suffix)
    return get_bcl2fastq_stats(stats_xml_file_path, aggregate_lanes, aggregate_reads)

###################### Other metrics #######################

def add_duplication_results(basecalls_dir, projects):
    for project in projects:
        for sample in project.samples:
            for f in sample.files:
                with open(samples) as metrics_file:
                    num_reads, num_dupes, num_dupes_dedup = metrics_file.read().strip().split("\t")
                    stats = f.stats or {}
                    stats['% Sequencing Duplicates (R1)'] = num_dupes_dedup * 100.0 / num_reads
                    f.stats = stats

