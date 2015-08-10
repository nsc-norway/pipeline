

from genologics.lims import *

# Local imports
import nsc
import utilities


# Sample object model
# Objects containing projects, samples and files.

class Project(object):
    """Project object.
    name: name with special characters replaced
    path: base name of project directory relative to Data/Intensities/BaseCalls
    samples: list of samples
    """
    def __init__(self, name, proj_dir, samples=[], is_undetermined=False):
        self.name = name
        self.proj_dir = proj_dir
        self.samples = samples
        self.is_undetermined = is_undetermined


class Sample(object):
    """Contains information about a sample. Contains a list of FastqFile
    objects representing the reads. One instance for each sample on a 
    flowcell, even if that sample is run on multiple lanes."""

    def __init__(self, name, files):
        self.name = name
        self.files = files


class Lane(object):
    """Represents a lane (physically separate sub-units of flowcells).
    
    For MiSeq and NextSeq there is only one lane (NextSeq's lanes aren't
    independent).."""

    def __init__(self, id, raw_cluster_density, pf_cluster_density, pf_ratio,
            is_merged=False):
        self.id = id
        self.raw_cluster_density = raw_cluster_density
        self.pf_cluster_density = pf_cluster_density
        self.pf_ratio = pf_ratio
        self.is_merged = is_merged


class FastqFile(object):
    """Represents a single output file for a specific sample, lane and
    read. Currently assumed to be no more than one FastqFile per read.
    
    lane is a Lane object containing lane-specific stats.

    read_num is the read index, 1 or 2 (second read is only available for paired
    end). Index reads are not considered.

    Path is the path to the fastq file relative to the "Unaligned"
    (bcl2fastq output) directory or Data/Intensities/BaseCalls.
    
    stats is a dict of stat name => value. See the functions which generate these
    stats below.
    
    empty is set by the QC function. You may set it, but it will be overwritten."""

    def __init__(self, lane, read_num, path, stats):
        self.lane = lane
        self.read_num = read_num
        self.path = path
        self.stats = stats
        self.empty = False



################ SAMPLE OBJECTS #################
def get_projects(sample_sheet_data):
    """Get the "sample object model" tree, one for each project.
    
    The FastqFile objects contain the file names after the post-demultiplexing
    "rename files" step has been run.
    
    Some information will be missing from the objects, this can be provided by 
    other functions later.
    
    In addition to the normal projects, an "Undetermined" project is also returned,
    to represent the undetermined index data."""
    for sample_index, entry in sample_sheet_data:
        sample_dir = ""


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
                fixed_sample_name = e['SampleId']
                path = path_t.format(fixed_sample_name, e['Index'], e['Lane'].zfill(3), ri)
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

    

    




################# SAMPLE SHEET ##################

def parse_sample_sheet_data(sample_sheet):
    lines = sample_sheet.splitlines()
    headers = [x.lower().replace("_", "") for x in lines[0].split(",")]
    samples = []
    for l in lines[1:]:
        sam = {}
        for h, v in zip(headers, l.split(",")):
            sam[h] = v
        samples.append(sam)
    return samples



def parse_sample_sheet(sample_sheet):
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


################# FILE STRUCTURE #################
def get_sample_name():
    pass


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


