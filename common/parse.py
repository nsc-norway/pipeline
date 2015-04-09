import re, os
from xml.etree import ElementTree
from collections import defaultdict


# Model: Objects containing projects, samples and files.
# Represents a unified interface to the QC functionality, similar to the
# SampleInformation.txt BarcodeLaneStatistics.txt from the perl scripts.

class Project(object):
    '''Project object.
    name: name with special characters replaced
    path: base name of project directory 
      path is relative to Unaligned for HiSeq, and relative to Data/Intensities/BaseCalls
      for MiSeq and NextSeq
    samples: list of samples
    '''
    def __init__(self, name, proj_dir, samples=[]):
        self.name = name
        self.proj_dir = proj_dir
        self.samples = samples


class Sample(object):
    '''Contains information about a sample. Contains a list of FastqFile
    objects representing the reads. One instance for each sample on a 
    flowcell, even if that sample is run on multiple lanes.'''

    def __init__(self, name, files):
        self.name = name
        self.files = files


class Lane(object):
    '''Represents a lane (physically separate sub-units of flowcells).
    
    For MiSeq and NextSeq there is only one lane (NextSeq's lanes aren't
    independent)..'''

    def __init__(self, id, raw_cluster_density, pf_cluster_density, pf_ratio):
        self.id = id
        self.raw_cluster_density = raw_cluster_density
        self.pf_cluster_density = pf_cluster_density
        self.pf_ratio = pf_ratio


class FastqFile(object):
    '''Represents a single output file for a specific sample, lane and
    read. Currently assumed to be no more than one FastqFile per read.
    
    lane is a Lane object containing lane-specific stats.

    read_num is the read index, 1 or 2 (second read is only available for paired
    end). Index reads are not considered.

    Path is the path to the fastq file relative to the "Unaligned"
    (bcl2fastq output) directory or Data/Intensities/BaseCalls.
    
    num_pf_reads is the number of full sequences that were read (number of clusters, 
    note the alternative meaning of "read").
    
    empty is set by the QC function. You may set it, but it will be overwritten.'''

    def __init__(self, lane, read_num, path, num_pf_reads, percent_of_pf_clusters):
        self.lane = lane
        self.read_num = read_num
        self.path = path
        self.num_pf_reads = num_pf_reads # Num_of_PF_Reads
        self.percent_of_pf_clusters = percent_of_pf_clusters
        self.empty = False




def parse_demux_stats(stats_data):
    '''Parse the Demultiplex_stats.htm file and return a list of records,
    one for each row.'''

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



def parse_demux_summary(demux_summary_file_path):
    '''Get lane-read-level demultiplexing statistics from
    Flowcell_demux_summary.xml.
    
    Statistics gathered:
        - per lane
        - per read (1/2)
        - pass filter / raw
    
    The lane_stats dict is quad-nested; by lane ID, read index (1/2),
    Pf / Raw and finally stat name.
    '''

    xmltree = ElementTree.parse(demux_summary_file_path)
    root = xmltree.getroot()
    tree = lambda: defaultdict(tree)
    total = tree()
    undetermined = tree()
    if root.tag != "Summary":
        raise RuntimeError("Expected XML element Summary, found " + root.tag)
    for lane in root.findall("Lane"):
        # Dicts indexed by read, for per-read-lane stats
        lane_id = lane.attrib['index']

        for sample in lane.findall("Sample"):
            for barcode in sample.findall("Barcode"):
                barcode_index = barcode.attrib['index']
                for tile in barcode.findall("Tile"):
                    for read in tile.findall("Read"):
                        read_id = read.attrib['index']
                        for filtertype in read:
                            ft = filtertype.tag
                            for stat in filtertype:
                                stat_val = total[lane_id][read_id][ft].get(stat.tag, 0)
                                add = int(stat.text)
                                total[lane_id][read_id][ft][stat.tag] = stat_val + add
                                if barcode_index == "Undetermined":
                                    un_val = undetermined[lane_id][read_id][ft].get(stat.tag, 0)
                                    undetermined[lane_id][read_id][ft][stat.tag] = un_val + add


    return total, undetermined


def parse_csv_sample_sheet(sample_sheet):
    lines = sample_sheet.splitlines()
    headers = lines[0].split(",")
    samples = []
    for l in lines[1:]:
        sam = {}
        for h, v in zip(headers, l.split(",")):
            sam[h] = v
        samples.append(sam)
    return samples


def parse_hiseq_sample_sheet(sample_sheet):
    return parse_csv_sample_sheet(sample_sheet)


def parse_ne_mi_seq_sample_sheet(sample_sheet):
    '''Returns a dict with keys header, reads, data. 

    header: dict of key-value pairs
    reads: list of number of cycles in each read
    data: list of samples
    '''

    # Will contain ['', Header 1, Data 1, Header 2, Data 2] where "header" are the 
    # things in []s
    sections = re.split(r"(\[\w+\])[\r\n]+", sample_sheet)
    result = {}
    for header, data in zip(sections[1::2], sections[2::2]):
        if header == "[Header]":
            result['header'] = {}
            for l in data.splitlines():
                parts = l.split(",")
                if len(parts) == 2:
                    result['header'][parts[0]] = parts[1]
        elif header == "[Reads]":
            result['reads'] = [int(c) for c in data.splitlines() if c.isdigit()]
        elif header == "[Data]":
            result['data'] = parse_csv_sample_sheet(data)

    return result


def get_hiseq_project_dir(run_id, project_name):
    '''Gets project directory name, prefixed by date and flowcell index'''
    date_machine_flowcell = re.match(r"([\d]+_[^_]+)_[\d]+_([AB])", run_id)
    project_prefix = date_machine_flowcell.group(1) + "." + date_machine_flowcell.group(2) + "."
    return project_prefix + "Project_" + project_name

def get_project_dir(run_id, project_name):
    '''Gets project directory name for mi and nextseq.'''
    date_machine = re.match(r"([\d]+_[^_]+)_", run_id)
    project_dir = date_machine.group(1) + ".Project_" + project_name
    return project_dir


# Support functions for get_hiseq_qc_data
def num(string, ntype=int):
    if string:
        return ntype(string.replace(",", ""))
    else:
        return ntype(0)


def get_hiseq_sw_versions(demultiplex_config):
    '''Get a dict with software names->versions.
    
    demultiplex_config is an Element object (from ElementTree)'''

    sw_versions = []
    # Software tags are nested (best way to see is to just look at the xml)
    sw_tags = [demultiplex_config.find('Software')]
    while sw_tags:
        tag = sw_tags.pop()
        sw_tags += tag.findall("Software")
        if tag.attrib['Name'] == "configureBclToFastq.pl": #special case
            name, ver = tag.attrib['Version'].split('-')
            sw_versions.append((name, ver))
        elif "RTA" in tag.attrib['Name']:
            sw_versions.append((tag.attrib['Name'], tag.attrib['Version']))

    return sw_versions


def get_hiseq_qc_data(run_id, n_reads, lanes, root_dir):
    '''Get HiSeq metadata about project, sample and files, including QC data. 
    Converted to the internal representation (model) classes defined above.

    n_reads is the number of sequence read passes, 1 or 2 (paired end)

    lanes is a dict with key: numeric lane number, value: lane object
    '''
    
    # Getting flowcell, software and sample information from DemultiplexConfig.xml
    # It has almost exactly the same data as the sample sheet, but it has the
    # advantage that it's always written by bcl2fastq, so we know that we're getting
    # the one that was used for demultiplexing.
    xmltree = ElementTree.parse(os.path.join(root_dir, "DemultiplexConfig.xml"))
    demultiplex_config = xmltree.getroot()

    sw_versions = get_hiseq_sw_versions(demultiplex_config)

    flowcell_info = demultiplex_config.find("FlowcellInfo")
    fcid = flowcell_info.attrib['ID']

    # List of samples
    samples = []
    for lane in flowcell_info.findall("Lane"):
        for sample in lane.findall("Sample"):
            sd = dict(sample.attrib)
            sd['Lane'] = lane.attrib['Number']
            samples.append(sd)

    # Project -> [Samples]
    project_entries = defaultdict(list)
    for sample_entry in samples:
        project_entries[sample_entry['ProjectId']].append(sample_entry)


    # Demultiplex_stats.htm contains most of the required information
    ds_path = os.path.join(root_dir, "Basecall_Stats_" + fcid, "Demultiplex_Stats.htm")
    demux_stats = parse_demux_stats(open(ds_path).read())

    projects = []
    for proj, entries in project_entries.items():
        if re.match("Undetermined_indices$", proj):
            project_dir = "Undetermined_indices"
        else:
            project_dir = get_hiseq_project_dir(run_id, proj)

        samples = []

        for e in entries:
            sample_dir = project_dir + "/Sample_" + e['SampleId']
            for stats in demux_stats:
                if stats['Lane'] == e['Lane'] and stats['Sample ID'] == e['SampleId']:
                    stats_entry = stats

            files = []
            for ri in xrange(1, n_reads + 1):

                # FastqFile
                path_t = sample_dir + "/{0}_{1}_L{2}_R{3}_001.fastq.gz"
                path = path_t.format(e['SampleId'], e['Index'], e['Lane'].zfill(3), ri) 
                lane = lanes[int(e['Lane'])]
                seq_reads = num(stats_entry['# Reads']) / n_reads
                f = FastqFile(lane, ri, path, seq_reads, num(stats_entry['% of raw clusters per lane'], float))
                # PF clusters = raw clusters because bcl2fastq doesn't save non-PF clusters
                if stats_entry['% PF'] != "100.00" and stats_entry['Yield (Mbases)'] != "0":
                    raise RuntimeError("Expected 100 % PF clusters, can't get the stats")

                files.append(f)

            s = Sample(e['SampleId'], files)
            samples.append(s)

        # Project 
        p = Project(proj, project_dir, samples)
        projects.append(p)

    info = {"sw_versions": sw_versions}

    return info, projects


def get_sw_versions(run_dir):
    '''Mainly for Mi/NextSeq'''

    try:
        xmltree = ElementTree.parse(os.path.join(run_dir, 'RunParameters.xml'))
    except IOError:
        xmltree = ElementTree.parse(os.path.join(run_dir, 'runParameters.xml'))

    run_parameters = xmltree.getroot()
    rta_ver = run_parameters.find("RTAVersion").text
    return {"RTA": rta_ver}



def get_ne_mi_seq_from_ssheet(run_id, run_dir, instrument, sample_sheet_path=None):
    '''Get NextSeq or MiSeq QC model objects.

    Gets the info from the sample sheet. Works for indexed projects and for 
    projects with no index, but with an entry in the sample sheet.
    
    This is limited compared to the HiSeq version -- many fields are filled
    with None because they are not available / we don't use them.'''

    if not sample_sheet_path:
        sample_sheet_path = os.path.join(run_dir, 'SampleSheet.csv')

    sample_sheet = parse_ne_mi_seq_sample_sheet(open(sample_sheet_path).read())
    n_reads = len(sample_sheet['reads'])
    project_name = sample_sheet['header']['Experiment Name']
    project_dir = get_project_dir(run_id, project_name)

    lane = Lane(1, None, None, None)
    if instrument == "miseq":
        file_lane_id = "1"
    elif instrument == "nextseq":
        file_lane_id = "X"


    samples = []
    for sam_index, sam in enumerate(sample_sheet['data']):
        files = []
        sample_name = sam['Sample_ID']
        for ir in xrange(1, n_reads+1):
            path = "{0}/{1}_S{2}_L00{3}_R{4}_001.fastq.gz".format(
                    project_dir, sample_name, str(sam_index + 1),
                    file_lane_id, str(ir))
            files.append(FastqFile(lane, ir, path, None, None))

        sample = Sample(sample_name, files)
        samples.append(sample)

    project = Project(project_name, project_dir, samples)

    unfiles = [
            FastqFile(lane, ir, "Undetermined_S0_L00%s_R%d_001.fastq.gz" % (file_lane_id, ir), None, None)
            for ir in xrange(1, n_reads + 1)
            ]
    unsample = Sample("Undetermined", unfiles)
    unproject = Project("Undetermined", None, [unsample]) 

    info = {'sw_versions': get_sw_versions(run_dir)}
    return info, [project, unproject]
    


def get_ne_mi_seq_from_files(run_dir):
    '''Get NextSeq or MiSeq QC "model" objects for run, without using sample
    sheet information.

    This function looks for project directories and extracts infromation from 
    the directory structure / file names. It is less likely to crash than the above 
    function, but also more likely to do something wrong.'''

    lane = Lane(1, None, None, None)

    projects = []
    basecalls_dir = os.path.join(run_dir, "Data", "intensities", "BaseCalls")
    projects_paths = glob.glob(os.path.join(basecalls_dir, "*_*.Project_*"))
    for pp in project_paths:
        p_dir = os.path.basename(pp)
        project_name = re.match("[\d]+_[A-Z0-9]+\.Project_(.*)", p_dir).group(1)

        sample_files = defaultdict(list)

        for filename in os.listdir(pp):
            parts = re.match("(.*?)_S\d+_L00X_R(\d+)_001.fastq.gz$", filename)
            if parts:
                sample_name = parts.group(1)
                read_n = int(parts.group(2))
                sample_files[sample_name].append((read_n, filename))

        samples = []
        for sample_name, files in sample_files.items():
            files = []
            for read_n, fname in files:
                f = FastqFile(lane, read_n, p_dir + "/" + fname)
                files.append(f)

            samples.append(Sample(sample_name, files))

        projects.append(Project(project_name, p_dir, samples))

    info = {'sw_versions': get_sw_versions(run_dir)}
    return info, projects




