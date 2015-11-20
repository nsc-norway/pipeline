# Parsing of bcl2fastq2 output, and Illumina Analysis Software output for the MiSeq

import re
import os
import itertools
import utilities
from xml.etree import ElementTree
from collections import defaultdict
from Counter import Counter


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
                                if iread == 192 or iread == 49: # Hack for samples without barcode: gets
                                                                # read == some large number (2 times now)
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
            stats['%PF'] = "100.0%"
        stats['% of Raw Clusters Per Lane'] = con_s_raw['ClusterCount'] * 100.0 / all_raw_reads[lane]
        stats['% of PF Clusters Per Lane'] = con_s_pf['ClusterCount'] * 100.0 / all_pf_reads[lane]
        if de_s['BarcodeCount'] != 0.0:
            stats['% Perfect Index Read'] = de_s['PerfectBarcodeCount'] * 100.0 / de_s['BarcodeCount']
            stats['% One Mismatch Reads (Index)'] = de_s['OneMismatchBarcodeCount'] * 100.0 / de_s['BarcodeCount']
        else:
            stats['% Perfect Index Read'] = 0
            stats['% One Mismatch Reads (Index)'] = 0
        stats['% Bases >=Q30'] = con_s_pf['YieldQ30'] * 100.0 / con_s_pf['Yield']
        stats['Ave Q Score'] = con_s_pf['QualityScoreSum'] * 1.0 / con_s_pf['Yield']
        result[coordinates] = stats

    return result





###################### MISEQ STATS #######################

def get_miseq_totals(run_stats):
    """Get the run totals for miseq. Argument is the RunStats XML element
    from the GenerateFASTQRunStatistics.xml file."""

    totals = {}
    for tag in [
            'NumberOfClustersPF',
            'NumberOfClustersRaw',
            'NumberOfUnalignedClusters',
            'NumberOfUnalignedClustersPF',
            'NumberOfUnindexedClusters',
            'NumberOfUnindexedClustersPF',
            ]:
        totals[tag] = int(run_stats.findall(tag)[0].text)

    return totals



def parse_generate_fastq(generate_fastq_path, num_reads=1, aggregate_reads=False): 
    """Get read-level demultiplexing statistics from GenerateFASTQRunStatistics.xml.
    
    (Modelled on code for the HiSeq)
    
    Statistics gathered:
        - per sample
        - pass filter / raw

    Per read stats not actually gathered, but we'll return multiple entries if
    per-read stats requested.
    
    The return value is:
    totals_dict, main_dict.

    
    totals_dict represents the stats for the full run (unaligned),
    main_dict represents the per-sample stats and undetermined.

    The main_dict is indexed by (sample_id,) (tuple with one element) if
    aggregate_reads is set, or (sample_id, read) if aggregate_reads is not set.
    { (lane, sample id,) => { stat name => value } }
    { (lane, sample id, read) => { stat name => value } }
    lane is always 1 (included to have same format as for HiSeq)
    
    Undetermined is represented as sample_id = None.
    """

    xmltree = ElementTree.parse(generate_fastq_path)
    root = xmltree.getroot()
    if root.tag != "StatisticsGenerateFASTQ":
        raise RuntimeError("Expected XML element StatisticsGenerateFASTQ, found " + root.tag)

    totals_dict = get_miseq_totals(root.findall("RunStats")[0])

    samples = {}
    # Undetermined first
    if aggregate_reads:
        indexes = [(1, None, 1)]
        factor = num_reads
    else:
        indexes = [(1, None, i) for i in range(1, num_reads+1)]
        factor = 1
    for index in indexes:
        stats = {}
        stats['NumberOfClustersRaw'] = totals_dict['NumberOfUnindexedClusters'] * factor
        stats['NumberOfClustersPF'] = totals_dict['NumberOfUnindexedClustersPF'] * factor
        samples[index] = stats

    overall = root.findall("OverallSamples")[0]
    for sample_xml in overall.findall("SummarizedSampleStatistics"):
        sample_id = sample_xml.findall("SampleID")[0].text

        # Needs to be changed if we actually get per-read stats later, but for
        # now they are just dummy values in the XML.
        if aggregate_reads:
            indexes = [(1, sample_id, 1)]
            factor = num_reads
        else:
            indexes = [(1, sample_id, i) for i in range(1, num_reads+1)]
            factor = 1

        sample_stats = {}
        for tag in [
                "NumberOfClustersRaw",
                "NumberOfClustersPF",
                ]:
            sample_stats[tag] = int(sample_xml.findall(tag)[0].text) * factor

        for index in indexes:
            samples[index] = sample_stats

    
    return totals_dict, samples



def get_miseq_stats(generate_fastq_path, num_reads, aggregate_reads):
    """Get the MiSeq demultiplexing stats.

    It returns a dict indexed by (sample ID,) or (sample ID, read)
    with the values being a dict indexed by the stats name:
    { (sample ID,) => {stat => value} }
    { (sample ID, read) => {stat => value} }

    Sample ID None represents Undetermined.

    No info is actually gathered per read, the stats are duplicated for 
    easier processing. The miseq info is quite limited compared to hi/nextseq, 
    but at least we get the number of clusters, which is the main thing that's 
    needed.
    """

    overall, samples = parse_generate_fastq(
            generate_fastq_path,
            num_reads,
            aggregate_reads
            )

    # Check: compute totals by summing up samples
    all_raw_clusters = sum(
            d["NumberOfClustersRaw"]
            for c,d in samples.items()
            if c[-1] == 1 # Read 1, or sum of reads if aggregate
            )
    all_pf_clusters = sum(
            d["NumberOfClustersPF"]
            for c,d in samples.items()
            if c[-1] == 1
            )

    # Check that stats for all samples add up to the totals
    if aggregate_reads:
        factor = num_reads
    else:
        factor = 1
    num_raw = overall["NumberOfClustersRaw"] * factor
    num_pf = overall["NumberOfClustersPF"] * factor
    assert(all_raw_clusters == num_raw)
    assert(all_pf_clusters == num_pf)

    result = {}
    for coordinates, xml_stats in samples.items():
        stats = {}
        try:
            # Note: if aggregate is set, this is no longer the actual number of clusters, but
            # the number of clusters times the number of read passes
            stats['# Reads'] = xml_stats['NumberOfClustersRaw']
            stats['# Reads PF'] = xml_stats['NumberOfClustersPF']
            if xml_stats['NumberOfClustersRaw'] > 0:
                stats['%PF'] = xml_stats['NumberOfClustersPF'] * 100.0 / xml_stats['NumberOfClustersRaw']
            else:
                stats['%PF'] = "100.0%"
            stats['% of Raw Clusters Per Lane'] = xml_stats['NumberOfClustersRaw'] * 100.0 / all_raw_clusters
            stats['% of PF Clusters Per Lane'] = xml_stats['NumberOfClustersPF'] * 100.0 / all_pf_clusters
        except ZeroDivisionError:
            print "Warning: division by zero"
        result[coordinates] = stats

    return result



###################### WRAPPER FNC #######################
def get_stats(
        instrument, 
        run_dir,
        aggregate_lanes=True,
        aggregate_reads=False,
        miseq_uniproject=None,
        suffix=""
        ):
    """Instrument-independent interfact to the stats module.

    MiSeq handling is kind of hackish because the stats file doesn't contain info
    on reads or project. I expect Illumina may start to use bcl2fastq2 for MiSeq too
    sooner or later, and then everything will be uniform.
    """

    try:
        stats_xml_file_path = os.path.join(run_dir, "Data", "Intensities", "BaseCalls", "Stats" + suffix)
        return get_bcl2fastq_stats(stats_xml_file_path, aggregate_lanes, aggregate_reads)
    except IOError:
        if instrument == 'miseq':
            generate_fastq_path = os.path.join(run_dir, "GenerateFASTQRunStatistics.xml")
            num_reads, index_reads = utilities.get_num_reads(run_dir)
            miseq_stats = get_miseq_stats(generate_fastq_path, num_reads, aggregate_reads)
            return dict((c[0:1] +
                (miseq_uniproject if c[1] else None,) + # <handling undetermined (sorry)
                c[1:], v)
                for c, v in miseq_stats.items())
        else:
            raise


