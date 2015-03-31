import re
from xml.etree import ElementTree
from collections import defaultdict





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

    tree = ElementTree.parse(demux_summary_file_path)
    root = tree.getroot()
    tree = lambda: defaultdict(tree)
    total = tree()
    undetermined = tree()
    if root.tag == "Summary":
        for lane in root.iterfind("Lane"):
            # Dicts indexed by read, for per-read-lane stats
            lane_id = lane.attrib['index']

            for sample in lane.iterfind("Sample"):
                for barcode in sample.iterfind("Barcode"):
                    barcode_index = barcode.attrib['index']
                    for tile in barcode.iterfind("Tile"):
                        for read in tile.iterfind("Read"):
                            read_id = read.attrib['index']
                            for filtertype in read:
                                ft = filtertype.tag
                                for stat in filtertype:
                                    total[lane_id][read_id][ft][stat.tag] += int(stat.text)
                                    if barcode_index == "Undetermined":
                                        undetermined[lane_id][read_id][ft][stat.tag] += int(stat.text)


    return total, undetermined



def parse_hiseq_sample_sheet(sample_sheet):
    lines = sample_sheet.splitlines()
    headers = lines[0].split(",")
    samples = []
    for l in lines[1:]:
        sam = {}
        for h, v in zip(headers, l.split(",")):
            sam[h] = v
        samples.append(sam)
    return samples



def get_hiseq_project_dir(run_id, project_name):
    '''Gets project directory name, prefixed by date and flowcell index'''
    date_machine_flowcell = re.match(r"([\d]+_[^_]+)_[\d]+_([AB])", run_id)
    project_prefix = date_machine_flowcell.group(1) + "." + date_machine_flowcell.group(2) + "."
    return project_prefix + "Project_" + project_name
    



# Model: Objects containing projects, samples and files.
# Represents a unified interface to the QC functionality, similar to the
# SampleInformation.txt BarcodeLaneStatistics.txt from the perl scripts.

class Project(object):
    '''Project object.
    name: name with special characters replaced
    path: base name of project directory (not full path)
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
    (bcl2fastq output) directory.
    
    num_pf_reads is the number of full sequences that were read (number of clusters, 
    note the alternative meaning of "read").'''

    def __init__(self, lane, read_num, path, num_pf_reads, percent_of_pf_clusters):
        self.lane = lane
        self.read_num = read_num
        self.path = path
        self.num_pf_reads = num_pf_reads # Num_of_PF_Reads
        self.percent_of_pf_clusters = percent_of_pf_clusters


class HiseqDemultiplexingUnit(object):
    '''Locations of key files for one run of bcl2fastq.'''
    def __init__(self, sample_sheet_data, basecall_stats, fastq_root):
        self.sample_sheet_data = sample_sheet_data
        self.basecall_stats = basecall_stats
        self.fastq_root = fastq_root


def num(string):
    return int(string.replace(",", ""))


def get_hiseq_qc_data(run_id, n_reads, demultiplexing_units):
    '''Get HiSeq metadata about project, sample and files, including QC data. 
    Converted to the internal representation (model) classes defined above.
    
    demultiplexing_units is a list of objects representing individual runs of
    bcl2fastq software. You may want to run the QC on multiple such units to 
    aggregate the data for a complete run.'''
    
    projects = []
    for u in demultiplexing_units:
        # Sample sheet is needed to map sample names to projects
        ssheet = parse_hiseq_sample_sheet(u.sample_sheet_data)
        project_entries = defaultdict(list)
        for sample_entry in ssheet:
            project_entries(sample_entry['SampleProject']).append(sample_entry)

        # Demultiplex_stats.htm contains most of the required information
        ds_data = os.path.join(u.basecall_stats, "Demultiplex_Stats.htm").read()
        demux_stats = parse_demux_stats(ds_data)

        for proj, entries in project_entries.items():
            project_dir = get_hiseq_project_dir(run_id, proj)

            samples = []

            for e in entries:
                sample_dir = project_dir + "/Sample_" + e['SampleID']
                for stats in demux_stats:
                    if stats['Lane'] == e['Lane'] and stats['Sample ID'] == e['SampleID']:
                        stats_entry = stats

                files = []
                for ri in xrange(1, n_reads):

                    # FastqFile
                    path_t = sample_dir + "/{sampleId}_{index}_L{lane}_R{readIndex}_001.fastq.gz"
                    path = path_t.format(e['SampleID'], e['Index'], e['Lane'].zfill(3), ri) 

                    f = FastqFile(int(e['Lane']), ri, path, num(stats_entry['# Reads']),
                            num(stats_entry['% of raw clusters per lane']))
                    # PF clusters = raw clusters because bcl2fastq doesn't save non-PF clusters
                    if stats_entry['% PF'] != "100.00" and stats_entry['Yield (Mbases)'] != "0":
                        raise RuntimeError("Expected 100 % PF clusters, can't get the stats")

                    files.append(f)

                s = Sample(e['SampleID'], files)

            # Project 
            path = os.path.join(u.fastq_root, "Proj_" + projname)
            p = Project(proj, project_dir, samples)
            projects.append(p)

    return projects

