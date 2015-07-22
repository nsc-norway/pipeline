# Parsing of bcl2fastq2 output and Illumina Analysis Software output for the MiSeq

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
    
    # We compute the most specific coordinate first, and then take the 
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
    """Function for the NextSeq, to compute the usual demultiplexing stats.

    Params: stats_xml_file_path: path to the directory containing the XML files
    (Data/Intensities/BaseCalls/Stats)

    aggregate_lanes:  add up stats for lanes (or average over, for percentages), and
                      only return coordinates with lane = "X"
    aggregate_reads:  add up reads, return read = 1 for all

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
    
    demultiplexing_stats = parse_demultiplexing_stats(
            os.path.join(stats_xml_file_path, "DemultiplexingStats.xml"),
            aggregate_lanes
            )
    conversion_stats = parse_conversion_stats(
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

