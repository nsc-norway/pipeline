import re
import os
import itertools
from xml.etree import ElementTree
from collections import defaultdict
from Counter import Counter


###################### HI/MISEQ METRICS #######################
def get_lane_cluster_density(path):
    """Get cluster density for lanes from report files in Data/reports.
    
    Returns a dict indexed by the 1-based lane number."""

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



###################### HISEQ METRICS #######################

def to_normal_dict(dd):
    '''Recursive function to convert a tree of defaultdict to a normal dict'''
    if isinstance(dd, defaultdict):
        return dict((k, to_normal_dict(v)) for k, v in dd.items())
    else:
        return dd


def parse_demux_summary(demux_summary_file_path, aggregate_reads):
    """Get lane-read-level demultiplexing statistics from
    Flowcell_demux_summary.xml (sum over tile, multiple barcodes per sample).
    
    Statistics gathered:
        - per lane
        - per sample
        - per read (1/2)
        - pass filter / raw
    
    The lane_stats dict is quad-nested; by lane ID(numeric), read index (1/2, int),
    Pf / Raw and finally stat name.
    """

    xmltree = ElementTree.parse(demux_summary_file_path)
    root = xmltree.getroot()
    tree = lambda: defaultdict(tree)
    total = tree()
    total = {}
    if root.tag != "Summary":
        raise RuntimeError("Expected XML element Summary, found " + root.tag)
    for lane in root.findall("Lane"):
        # Dicts indexed by read, for per-read-lane stats
        lane_id = int(lane.attrib['index'])

        for sample in lane.findall("Sample"):
            sample_name = sample.attrib['index']
            for barcode in sample.findall("Barcode"):
                barcode_index = barcode.attrib['index']
                for tile in barcode.findall("Tile"):
                    for read in tile.findall("Read"):
                        read_id = int(read.attrib['index'])
                        if aggregate_reads:
                            key = (lane_id, sample_name, read_id)
                        else:
                            key = (lane_id, sample_name, read_id)
                        if not total.has_key(key):
                            total[key] = defaultdict(dict)
                        for filtertype in read:
                            ft = filtertype.tag
                            for stat in filtertype:
                                stat_val = total[key][ft].get(stat.tag, 0)
                                total[key][ft][stat.tag] = stat_val + int(stat.text)

    return to_normal_dict(total)


def get_hiseq_stats(demux_summary_file_path, aggregate_reads=False):
    """Get the standard demultiplexing statistics for HiSeq based on the data in
    Flowcell_demux_summary.xml.
    
    Returns dict indexed by (lane, sample id, read).


    See also: get_nextseq_stats.
    """

    # each demux summary item has "coordinates", which is (lane, sample, read) or
    # (lane, sample) if aggregate_reads is set
    demux_summary = parse_demux_summary(demux_summary_file_path, aggregate_reads)
    result = {}
    # lane_sum_clusters: { lane_id => number of clusters in lane } (for percentage per file)
    # Uses a single read, as # clusters is the same for all reads
    lane_sum_pf_clusters = defaultdict(int)
    lane_sum_raw_clusters = defaultdict(int)
    for coordinates, stats in demux_summary.items():
        # First read if not aggregate (real number of clusters then)
        # or sum of reads if doing the aggregate (note: gives 
        # different results, if aggregate is set we sum up the number of 
        # clusters)
        if len(coordinates) <= 2 or coordinates[2] == 1:
            lane = coordinates[0]
            lane_sum_pf_clusters[lane] += stats['Pf']['ClusterCount']
            lane_sum_raw_clusters[lane] += stats['Raw']['ClusterCount']

    result = {}
    for coordinates, readstats in demux_summary.items():
        lane_id = coordinates[0]
        pf = readstats['Pf']
        raw = readstats['Raw']
        stats = {}
        stats['# Reads'] = raw['ClusterCount']
        stats['# Reads PF'] = pf['ClusterCount']
        stats['Yield PF (Gb)'] = raw['Yield'] / 1e9
        if raw['ClusterCount'] > 0:
            stats['%PF'] = pf['ClusterCount'] * 100.0 / raw['ClusterCount']
        else:
            stats['%PF'] = "100.0"
        stats['% of Raw Clusters Per Lane'] =\
                raw['ClusterCount'] * 100.0 / lane_sum_raw_clusters[lane_id]
        stats['% of PF Clusters Per Lane'] =\
                pf['ClusterCount'] * 100.0 / lane_sum_pf_clusters[lane_id]
        stats['% Perfect Index Read'] =\
                pf['ClusterCount0MismatchBarcode'] * 100.0 / pf['ClusterCount']
        stats['% One Mismatch Reads (Index)'] =\
                pf['ClusterCount1MismatchBarcode'] * 100.0 / pf['ClusterCount']
        stats['% Bases >=Q30'] = pf['YieldQ30'] * 100.0 / pf['Yield']
        stats['Ave Q Score'] = pf['QualityScoreSum'] / pf['Yield']
        
        result[coordinates] = stats

    return result




###################### NEXTSEQ METRICS #######################

def parse_conversion_stats(conversion_stats_path, aggregate_lanes, aggregate_reads):
    """Get "conversion stats" from the NextSeq stats.

    Loops over the comprehensive tree structure of the ConversionStats.xml
    file and generates the grand totals. 

    Data are returned in a dict indexed by sample name and read number (1/2). Stats
    which are the same for both reads are replicated in the values for both reads.

    {
        (coordinates) => ( {sample stats}, {sample stats PF} )
    }

    Depending on the aggregate_X parameters, the coordinates can be:
     ("X", sample name, 1)
     (lane, sample name, 1)
     (lane, sample name, read)

    Note: aggregating over lane but not read number is supported, but other code will
    expect that lane is the first parameter if len(coordinates) > 1.

    Undetermined indexes are reported as sample name = None.
    """
    xmltree = ElementTree.parse(conversion_stats_path)
    root = xmltree.getroot()
    if root.tag != "Stats":
        raise RuntimeError("Expected XML element Stats, found " + root.tag)
    fc = root.find("Flowcell")

    project = None
    for pro in fc.findall("Project"):
        if pro.attrib['name'] == "default":
            default_project = pro
        elif pro.attrib['name'] != "all":
            project = pro

    if not project:
        project = default_project
    
    # We compute the most general coordinate first, and then take the 
    # aggregates (sum) later.
    samples = {}
    for is_undetermined, project in [(False, project), (True, default_project)]:
        for sample in project.findall("Sample"):
            if is_undetermined:
                sample_id = None
                if not sample.attrib['name'] == "unknown":
                    continue
            else:
                sample_id = sample.attrib['name']
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
                                if not rst.has_key(iread):
                                    rst[iread] = defaultdict(int)

                                for stat in read_or_cc:
                                    rst[iread][stat.tag] += int(stat.text)

                lane_id = int(lane.attrib['number'])
                for iread in read_stats_pf.keys():
                    # Include the read-independent stats into per-read stats
                    read_stats_pf[iread].update(stats_pf)
                    read_stats_raw[iread].update(stats_raw)
                    samples[(lane_id, sample_id, iread)] = (read_stats_raw[iread], read_stats_pf[iread])

    # sorry
    if aggregate_lanes:
        # sorted samples by 
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
        # sorted samples by 
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


def parse_ns_demultiplexing_stats(conversion_stats_path, aggregate_lanes):
    """Sum up "demultiplexing stats" from the NextSeq stats directory.
    
    Contains information about barcode reads for the NextSeq. The returned 
    dict is indexed by the sample name, and it also includes an entry for 
    sample None, which corresponds to undetermined indexes.
    
    Returns: {
         (lane, sample name) => {stats}
    } 
    or {
         ("X", sample name) => {stats}
    } 
    """
    xmltree = ElementTree.parse(conversion_stats_path)
    root = xmltree.getroot()
    if root.tag != "Stats":
        raise RuntimeError("Expected XML element Stats, found " + root.tag)
    fc = root.find("Flowcell")
    # There are always two projects, "default" and "all". This code must be revised
    # if NS starts allowing independent projects.
    project = None
    for pro in fc.findall("Project"):
        if pro.attrib['name'] == "default":
            default_project = pro
        elif pro.attrib['name'] != "all":
            project = pro

    if not project:
        project = default_project

    # Real samples
    samples = {}
    for sample in project.findall("Sample"):
        sample_id = sample.attrib['name']
        # only look at barcode="all"
        barcode = next(bar for bar in sample.findall("Barcode") if bar.attrib['name'] == 'all')
        if aggregate_lanes:
            stats = defaultdict(int)
            key = ("X", sample_id)
        for lane in barcode.findall("Lane"):
            if not aggregate_lanes:
                stats = defaultdict(int)
                key = (int(lane.attrib['number']), sample_id)

            for stat in lane:
                stats[stat.tag] += int(stat.text)

            samples[key] = stats

        samples[sample_id] = stats
    # Undetermined
    sample = next(sam for sam in default_project.findall("Sample") if sam.attrib['name'] == "unknown")
    barcode = next(bar for bar in sample.findall("Barcode") if bar.attrib['name'] == 'all')
    stats = defaultdict(int)
    for lane in barcode.findall("Lane"):
        if aggregate_lanes:
            key = ("X", None)
        else:
            key = (int(lane.attrib['number']), None)
            stats = defaultdict(int)

        for stat in lane:
            stats[stat.tag] += int(stat.text)

        samples[key] = stats

    return samples


def get_nextseq_stats(stats_xml_file_path, aggregate_lanes=True, aggregate_reads=False):
    """Function for the NextSeq, to compute the canonical stats for individual 
    output files -- one per sample per read.

    Params: stats_xml_file_path: path to the directory containing the XML files
    (Data/Intensities/BaseCalls/Stats)

    This function computes derived statistics based on the accumulated data 
    from the two above functions. 

    It returns a dict indexed by lane, sample ID, and read, with the values
    being a dict indexed by the stats name:
    { (lane (=1), sample name, read) => {stat => value} }
    """

    if aggregate_lanes:
        lanes = ("X",)
    else:
        lanes = (1,2,3,4)
    
    demultiplexing_stats = parse_ns_demultiplexing_stats(
            os.path.join(stats_xml_file_path, "DemultiplexingStats.xml"),
            aggregate_lanes
            )
    conversion_stats = parse_ns_conversion_stats(
            os.path.join(stats_xml_file_path, "ConversionStats.xml"),
            aggregate_lanes, aggregate_reads
            )

    # Totals
    all_raw_reads = dict(
            (lane, conversion_stats[(lane, "all", 1)][0]['ClusterCount'] +
                conversion_stats[(lane, None, 1)][0]['ClusterCount'])
            for lane in lanes
            )
    all_pf_reads = dict(
            (lane, conversion_stats[(lane, "all", 1)][1]['ClusterCount'] +
                conversion_stats[(lane, None, 1)][1]['ClusterCount'])
            for lane in lanes
            )

    result = {}
    for coordinates in conversion_stats.keys():
        lane, sample, read = coordinates
        de_s = demultiplexing_stats[(lane, sample)]
        con_s_raw, con_s_pf = conversion_stats[coordinates]

        stats = {}
        try:
            stats['# Reads'] = con_s_raw['ClusterCount']
            stats['# Reads PF'] = con_s_pf['ClusterCount']
            stats['Yield PF (Gb)'] = con_s_pf['Yield'] / 1e9
            if con_s_raw['ClusterCount'] > 0:
                stats['%PF'] = con_s_pf['ClusterCount'] * 100.0 / con_s_raw['ClusterCount']
            else:
                stats['%PF'] = "100.0%"
            stats['% of Raw Clusters Per Lane'] = con_s_raw['ClusterCount'] * 100.0 / all_raw_reads[lane]
            stats['% of PF Clusters Per Lane'] = con_s_pf['ClusterCount'] * 100.0 / all_pf_reads[lane]
            stats['% Perfect Index Read'] = de_s['PerfectBarcodeCount'] * 100.0 / de_s['BarcodeCount']
            stats['% One Mismatch Reads (Index)'] = de_s['OneMismatchBarcodeCount'] * 100.0 / de_s['BarcodeCount']
            stats['% Bases >=Q30'] = con_s_pf['YieldQ30'] * 100.0 / con_s_pf['Yield']
            stats['Ave Q Score'] = con_s_pf['QualityScoreSum'] / con_s_pf['Yield']
        except ZeroDivisionError:
            print "Warning: division by zero"
        result[coordinates] = stats

    return result


###################### MISEQ METRICS!! #######################

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
    for i_read in range(1, num_reads+1):
        stats = {}
        stats['NumberOfClustersRaw'] = totals_dict['NumberOfUnindexedClusters']
        stats['NumberOfClustersPF'] = totals_dict['NumberOfUnindexedClustersPF']
        samples[(1, None, i_read)] = stats

    overall = root.findall("OverallSamples")[0]
    for sample_xml in overall.findall("SummarizedSampleStatistics"):
        sample_id = sample_xml.findall("SampleID")[0].text

        # Needs to be changed if we actually get per-read stats later, but for
        # now they are just dummy values in the XML.
        if aggregate_reads:
            indexes = [(sample_id,)]
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




