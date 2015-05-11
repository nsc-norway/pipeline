import re
import os
from xml.etree import ElementTree
from collections import defaultdict


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

def parse_demux_stats(stats_data):
    """Parse the Demultiplex_stats.htm file and return a list of records,
    one for each row."""

    # re.DOTALL does a "tall" match, including multiple lines
    tables = re.findall("<table[ >].*?</table>", stats_data, re.DOTALL)

    header_table = tables[0]
    field_names = []
    for match in re.finditer("<th>(.*?)</th>", header_table):
        field_names.append(match.group(1))

    barcode_lane_table = tables[1]
    barcode_lanes = []
    for row in re.finditer("<tr>(.*?)</tr>", barcode_lane_table, re.DOTALL): 
        barcode_lane = {}
        for i, cell in enumerate(re.finditer("<td>(.*?)</td>", row.group(1))):
            barcode_lane[field_names[i]] = cell.group(1)
        barcode_lanes.append(barcode_lane)

    return barcode_lanes


def to_normal_dict(dd):
    '''Recursive function to convert a tree of defaultdict to a normal dict'''
    if isinstance(dd, defaultdict):
        return dict((k, to_normal_dict(v)) for k, v in dd.items())
    else:
        return dd


def parse_demux_summary(demux_summary_file_path):
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
                        key = (lane_id, sample_name, read_id)
                        if not total.has_key(key):
                            total[key] = defaultdict(dict)
                        for filtertype in read:
                            ft = filtertype.tag
                            for stat in filtertype:
                                stat_val = total[key][ft].get(stat.tag, 0)
                                total[key][ft][stat.tag] = stat_val + int(stat.text)
    

    return to_normal_dict(total)


def get_hiseq_stats(demux_summary_file_path):
    """Get the standard demultiplexing statistics for HiSeq based on the data in
    Flowcell_demux_summary.xml.
    
    Returns dict indexed by (lane, sample id, read).


    See also: get_nextseq_stats.
    """

    demux_summary = parse_demux_summary(demux_summary_file_path)
    result = {}
    # lane_sum_clusters: { lane_id => number of clusters in lane } (for percentage per file)
    # Uses a single read, as # clusters is the same for all reads
    lane_sum_pf_clusters = defaultdict(int)
    lane_sum_raw_clusters = defaultdict(int)
    for (lane, sample_id, iread), stats in demux_summary.items():
        if iread == 1:
            lane_sum_pf_clusters[lane] += stats['Pf']['ClusterCount']
            lane_sum_raw_clusters[lane] += stats['Raw']['ClusterCount']

    result = {}
    for (lane_id, sample_id, iread), readstats in demux_summary.items():
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
        
        result[(lane_id, sample_id, iread)] = stats

    return result




###################### NEXTSEQ METRICS #######################

def parse_ns_conversion_stats(conversion_stats_path):
    """Get "conversion stats" from the NextSeq stats.

    Loops over the comprehensive tree structure of the ConversionStats.xml
    file and generates the grand totals. 

    Data are returned in a dict indexed by sample name and read number (1/2). Stats
    which are the same for both reads are replicated in the values for both reads.

    {
        (Sample name, read) => ( {sample stats}, {sample stats PF} )
    }
    """
    xmltree = ElementTree.parse(conversion_stats_path)
    root = xmltree.getroot()
    if root.tag != "Stats":
        raise RuntimeError("Expected XML element Stats, found " + root.tag)
    fc = root.find("Flowcell")
    project = next(pro for pro in fc.findall("Project")
            if pro.attrib['name'] != "all" and pro.attrib['name'] != "default")
    samples = {}
    for sample in project.findall("Sample"):
        sample_id = sample.attrib['name']
        # stats_* are indexed by filter type (pf/raw) then by the read index (then will
        # be re-organised in the return value)
        stats_pf = defaultdict(int)
        stats_raw = defaultdict(int)
        read_stats_pf = {}
        read_stats_raw = {}
        barcode = next(bar for bar in sample.findall("Barcode") if bar.attrib['name'] == 'all')
        for lane in barcode.findall("Lane"):
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

        for iread in read_stats_pf.keys():
            read_stats_pf[iread].update(stats_pf)
            read_stats_raw[iread].update(stats_raw)
            samples[(sample_id, iread)] = (read_stats_raw[iread], read_stats_pf[iread])

    return samples


def parse_ns_demultiplexing_stats(conversion_stats_path):
    """Sum up "demultiplexing stats" from the NextSeq stats directory.
    
    Contains information about barcode reads for the NextSeq.
    
    Returns: {
         (sample name) => {stats}
    }  """
    xmltree = ElementTree.parse(conversion_stats_path)
    root = xmltree.getroot()
    if root.tag != "Stats":
        raise RuntimeError("Expected XML element Stats, found " + root.tag)
    fc = root.find("Flowcell")
    # There are always two projects, "default" and "all". This code must be revised
    # if NS starts allowing independent projects, but for now we can just as easily get
    # the total sum of stats from the sample called "all" in the default project.
    project = next(pro for pro in fc.findall("Project") if pro.attrib['name'] != "all" and pro.attrib['name'] != "default")
    samples = {}
    for sample in project.findall("Sample"):
        sample_id = sample.attrib['name']
        stats = defaultdict(int)
        # only look at barcode="all"
        barcode = next(bar for bar in sample.findall("Barcode") if bar.attrib['name'] == 'all')
        for lane in barcode.findall("Lane"):
            for stat in lane:
                stats[stat.tag] += int(stat.text)

        samples[sample_id] = stats
    return samples


def get_nextseq_stats(stats_xml_file_path):
    """Function for the NextSeq, to compute the canonical stats for individual 
    output files -- one per sample per read.

    Params: stats_xml_file_path: path to the directory containing the XML files
    (Data/Intensities/BaseCalls/Stats)

    This function computes derived statistics based on the accumulated data 
    from the two above functions. The statistics are used in UDFs and the QC
    reporting, and are similar to those given in Demultiplex_stats.htm 
    (but that file is no longer used).

    It returns a dict indexed by lane, sample ID, and read, with the values
    being a dict indexed by the stats name:
    { (lane (=1), sample name, read) => {stat => value} }
    """
    
    demultiplexing_stats = parse_ns_demultiplexing_stats(
            os.path.join(stats_xml_file_path, "DemultiplexingStats.xml")
            )
    conversion_stats = parse_ns_conversion_stats(
            os.path.join(stats_xml_file_path, "ConversionStats.xml")
            )

    all_raw_reads = conversion_stats[("all", 1)][0]['ClusterCount']
    all_pf_reads = conversion_stats[("all", 1)][1]['ClusterCount']
    result = {}
    for sample,read in conversion_stats.keys():

        de_s = demultiplexing_stats[(sample)]
        con_s_raw, con_s_pf = conversion_stats[(sample,read)]

        stats = {}
        stats['# Reads'] = con_s_raw['ClusterCount']
        stats['# Reads PF'] = con_s_pf['ClusterCount']
        stats['Yield PF (Gb)'] = con_s_pf['Yield'] / 1e9
        if con_s_raw['ClusterCount'] > 0:
            stats['%PF'] = con_s_pf['ClusterCount'] * 100.0 / con_s_raw['ClusterCount']
        else:
            stats['%PF'] = "100.0%"
        stats['% of Raw Clusters Per Lane'] = con_s_raw['ClusterCount'] * 100.0 / all_raw_reads
        stats['% of PF Clusters Per Lane'] = con_s_pf['ClusterCount'] * 100.0 / all_pf_reads
        stats['% Perfect Index Read'] = de_s['PerfectBarcodeCount'] * 100.0 / de_s['BarcodeCount']
        stats['% One Mismatch Reads (Index)'] = de_s['OneMismatchBarcodeCount'] * 100.0 / de_s['BarcodeCount']
        stats['% Bases >=Q30'] = con_s_pf['YieldQ30'] * 100.0 / con_s_pf['Yield']
        stats['Ave Q Score'] = con_s_pf['QualityScoreSum'] / con_s_pf['Yield']
        result[(1, sample, read)] = stats

    return result



################# SAMPLE SHEET ##################

def parse_csv_sample_sheet(sample_sheet):
    lines = sample_sheet.splitlines()
    headers = [x.lower() for x in lines[0].split(",")]
    samples = []
    for l in lines[1:]:
        sam = {}
        for h, v in zip(headers, l.split(",")):
            sam[h.replace("_", "")] = v
        samples.append(sam)
    return samples


def parse_hiseq_sample_sheet(sample_sheet):
    return parse_csv_sample_sheet(sample_sheet)


def parse_ne_mi_seq_sample_sheet(sample_sheet):
    """Returns a dict with keys header, reads, data. 

    header: dict of key-value pairs
    reads: list of number of cycles in each read
    data: list of samples
    """

    # Will contain ['', Header 1, Data 1, Header 2, Data 2] where "header" are the 
    # things in []s
    sections = re.split(r"(\[\w+\])[,\r\n]+", sample_sheet)
    # If sample sheet is edited in Excel it will contain commas after the [Header],,,
    result = {}
    for header, data in zip(sections[1::2], sections[2::2]):
        if header == "[Header]":
            result['header'] = {}
            for l in data.splitlines():
                parts = l.split(",")
                if len(parts) >= 2:
                    result['header'][parts[0]] = parts[1]
        elif header == "[Reads]":
            result['reads'] = []
            for line in data.splitlines():
                c = line.strip(",")
                if c.isdigit() and not int(c) == 0:
                    result['reads'].append(int(c))
        elif header == "[Data]":
            result['data'] = parse_csv_sample_sheet(data)

    return result


################# DIRECTORY STRUCTURE #################

def get_hiseq_project_dir(run_id, project_name):
    """Gets project directory name, prefixed by date and flowcell index"""
    date_machine_flowcell = re.match(r"([\d]+_[^_]+)_[\d]+_([AB])", run_id)
    project_prefix = date_machine_flowcell.group(1) + "." + date_machine_flowcell.group(2) + "."
    return project_prefix + "Project_" + project_name


def get_project_dir(run_id, project_name):
    """Gets project directory name for mi and nextseq."""
    date_machine = re.match(r"([\d]+_[^_]+)_", run_id)
    project_dir = date_machine.group(1) + ".Project_" + project_name
    return project_dir


