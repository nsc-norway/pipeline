import os
import glob
import re
import gzip

from genologics.lims import *

# Local imports
import nsc
import utilities


# Sample object model
# Objects containing projects, samples and files.

class Project(object):
    """Project object.
    name: name with special characters replaced
    proj_dir: base name of project directory relative to Data/Intensities/BaseCalls
    samples: list of samples
    """
    def __init__(self, name, proj_dir, samples, is_undetermined=False, is_default=False):
        self.name = name
        self.proj_dir = proj_dir
        self.samples = samples
        self.is_undetermined = is_undetermined
        self.is_default = is_default


class Sample(object):
    """Contains information about a sample. Contains a list of FastqFile
    objects representing the reads. One instance for each sample on a 
    flowcell, even if that sample is run on multiple lanes.
    
    sample_index is the 1-based index of the entry in the sample sheet,
    counting only unique samples
    
    """

    def __init__(self, sample_index, sample_id, name, sample_dir, files):
        self.sample_index = sample_index
        self.sample_id = sample_id
        self.name = name
        self.sample_dir = sample_dir
        self.files = files


class FastqFile(object):
    """Represents a single output file for a specific sample, lane and
    read. Currently assumed to be no more than one FastqFile per read.

    lane is an integer representing the lane, or "X" for merged lanes

    i_read is the read ordinal, 1 or 2 (second read is only available for paired
    end). Index reads are not considered.

    Path is the path to the fastq file relative to the "Unaligned"
    (bcl2fastq output) directory or Data/Intensities/BaseCalls.
    
    stats is a dict of stat name => value. See the functions which generate these
    stats below.
    
    empty is set by the QC function. You may set it, but it will be overwritten."""

    def __init__(self, lane, i_read, filename, path, index_sequence, stats):
        self.lane = lane
        self.i_read = i_read
        self.path = path
        self.filename = filename
        self.stats = stats
        self.index_sequence = index_sequence
        self.empty = False



################ Get object tree, with various info #################
def get_projects(run_id, sample_sheet_data, num_reads, merged_lanes, expand_lanes=[1], experiment_name=None):
    """Get the "sample object model" tree, one for each project.
    
    The FastqFile objects contain the file names after the post-demultiplexing
    "rename files" step has been run.
    
    Some information will be missing from the objects, such as the stats, this
    can be provided by other functions later.
    
    In addition to the normal projects, an "Undetermined" project is also returned,
    to represent the undetermined index data.
    
    Arguments:
    run_id
    sample_sheet_data   - [Data] section of sample sheet as a list of dicts
    num_reads           - number of read passes 1=single read, 2=paired end
    merged_lanes        - combine all lanes into one lane with ID "X"
    expand_lanes        - when merged_lanes=False and sample sheet doesn't contain
                          a lane number, copy all samples on these lanes (list)
    """ 

    projects = {}
    lanes = set()
    instrument = utilities.get_instrument_by_runid(run_id)
    sample_index = 1
    for entry in sample_sheet_data:
        project_name = entry.get('project') or entry.get('sampleproject')
        default_project = False
        if not project_name:
            if experiment_name:
                project_name = experiment_name
                default_project = True
            else:
                raise RuntimeError("Project name not found in sample sheet")
        project = projects.get(project_name)
        if not project:
            project_dir = get_project_dir(run_id, project_name) # fn defined in bottom of this file
            project = Project(project_name, project_dir, [], is_default=default_project)
            projects[project_name] = project

        for sample in project.samples:
            if sample.sample_id == entry['sampleid']:
                break
        else: # if not break
            sample_name = entry['samplename']
            if sample_name == "":
                # MiSeq only uses Sample ID (at least for non-LIMS sample sheet)
                sample_name = entry['sampleid']
            sample_dir = get_sample_dir(instrument, sample_name)
            sample = Sample(sample_index, entry['sampleid'], sample_name, sample_dir, [])
            sample_index += 1
            project.samples.append(sample)

        if merged_lanes:
            file_lanes = ["X"]
        else:
            try:
                file_lanes = [int(entry['lane'])]
            except KeyError:
                file_lanes = expand_lanes

        for lane_id in file_lanes:
            lanes.add(lane_id)

            path = ""
            if project.proj_dir:
                path = project.proj_dir + "/"
            if sample.sample_dir:
                path += sample.sample_dir + "/"

            for i_read in xrange(1, num_reads+1):

                fastq_name = get_fastq_name(
                        instrument, 
                        sample.name,
                        sample.sample_index,
                        entry.get('index'),
                        entry.get('index2'),
                        lane_id,
                        i_read,
                        merged_lanes
                        )

                # path contains trailing slash
                fastq_path = path + fastq_name

                index_sequence = entry.get("index")
                if entry.has_key("index2"):
                    index_sequence += "-" + entry.get("index2")

                sample.files.append(FastqFile(lane_id, i_read, fastq_name, fastq_path, index_sequence, None))
                # Stats can be added in later

    # Create an undetermined file for each lane, read seen
    undetermined_project = Project(None, None, [], True)
    undetermined_sample = Sample(0, None, None, None, [])
    undetermined_project.samples.append(undetermined_sample)
    for lane in lanes:
        for i_read in xrange(1, num_reads+1):
            if merged_lanes:
                path = "Undetermined_S0_R{0}_001.fastq.gz".format(i_read)
            else:
                path = "Undetermined_S0_L{0}_R{1}_001.fastq.gz".format(
                        str(lane).zfill(3), i_read
                        )
            undetermined_sample.files.append(FastqFile(lane, i_read, path, path, None, None))

    return [undetermined_project] + projects.values()


def check_files_merged_lanes(run_dir):
    basecalls_dir = os.path.join(run_dir, "Data", "Intensities", "BaseCalls")
    unmerged_exists = len(glob.glob(basecalls_dir + "/Undetermined_S0_L*_R1_001.fastq.gz")) > 0
    merged_exists = os.path.exists(basecalls_dir + "/Undetermined_S0_R1_001.fastq.gz")
    if merged_exists and not unmerged_exists:
        return True
    elif unmerged_exists and not merged_exists:
        return False
    else:
        raise RuntimeError("Unable to determine if lanes were merged (no-lane-splitting option)")


def add_stats(projects, run_stats):
    """Adds the stats from the stats module to the appropriate
    FastqFile objects in the tree structure produced by the above 
    function.
    """

    for project in projects:
        for sample in project.samples:
            for f in sample.files:
                stats = run_stats.get((f.lane, project.name, sample.name, f.i_read))
                if stats:
                    f.stats = stats


def flag_empty_files(projects, run_dir):
    basecalls_dir = os.path.join(run_dir, "Data", "Intensities", "BaseCalls")
    for p in projects:
        for s in p.samples:
            for f in s.files:
                full_path = os.path.join(basecalls_dir, f.path)
                f.empty = not os.path.exists(full_path)
                if not f.empty:
                    # Additional check for empty gzip file
                    gzfile = gzip.open(full_path, 'rb')
                    data = gzfile.read(1)
                    f.empty = data == ""


################# SAMPLE SHEET ##################
# Low level sample sheet parsing

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
            result['data'] = parse_sample_sheet_data(data)

    return result


################# FILE STRUCTURE #################
def get_project_dir(run_id, project_name):
    if utilities.get_instrument_by_runid(run_id) == 'hiseq':
        return get_hiseq_project_dir(run_id, project_name)
    else:
        return get_ne_mi_project_dir(run_id, project_name)

def get_hiseq_project_dir(run_id, project_name):
    """Gets project directory name, prefixed by date and flowcell index"""
    date_machine_flowcell = re.match(r"([\d]+_[^_]+)_[\d]+_([AB])", run_id)
    project_prefix = date_machine_flowcell.group(1) + "." + date_machine_flowcell.group(2) + "."
    return project_prefix + "Project_" + project_name


def get_ne_mi_project_dir(run_id, project_name):
    """Gets project directory name for mi and nextseq."""
    date_machine = re.match(r"([\d]+_[^_]+)_", run_id)
    project_dir = date_machine.group(1) + ".Project_" + project_name
    return project_dir


def get_sample_dir(instrument, sample_name):
    if instrument == 'hiseq':
        return "Sample_" + sample_name
    else:
        return None


def get_fastq_name(instrument, sample_name, sample_index,
        index1, index2, lane_id, i_read, merged_lanes):
    """The file name we want depends on the instument type, for consistency with older
    deliveries."""
    
    if instrument == "hiseq":
        if index1:
            index_seq = index1
        else:
            index_seq = "NoIndex"
        if index2:
            index_seq += "-" + index2
        name = "{0}_{1}_L{2}_R{3}_001.fastq.gz".format(
                sample_name,
                index_seq,
                str(lane_id).zfill(3),
                i_read)

    
    else:
        return bcl2fastq2_file_name(sample_name, sample_index, lane_id, i_read, merged_lanes)

    return name
    

def bcl2fastq2_file_name(sample_name, sample_index, lane_id, i_read, merged_lanes):
    if merged_lanes:
        name = "{0}_S{1}_R{2}_001.fastq.gz".format(
                sample_name,
                sample_index, i_read
                )
    else:
        name = "{0}_S{1}_L{2}_R{3}_001.fastq.gz".format(
                sample_name,
                sample_index, str(lane_id).zfill(3),
                i_read)
    return name

    

def get_fastqc_dir(project, sample, fastqfile):
    """Get the directory in which the fastqc results are stored
    (after moving it)."""

    fqc_name = re.sub(r".fastq.gz$", "_fastqc", fastqfile.filename)
    if project.name:
        project_name = project.name
        sample_name = sample.name
    else:
        project_name = "Undetermined"
        sample_name = "Undetermined"
    return os.path.join(project_name, "Sample_" + sample_name, fqc_name)


def qc_pdf_name(run_id, fastq):
    """Get QC report name for a given FastqFile object"""
    report_root_name = re.sub(".fastq.gz$", ".qc", os.path.basename(fastq.path))
    miseq = utilities.get_instrument_by_runid(run_id) == "miseq"
    if fastq.lane == "X" or miseq: # Merged lanes or single-lane instrument
        return "{0}.{1}.pdf".format(run_id, report_root_name)
    else:
        return "{0}.{1}.{2}.pdf".format(run_id, fastq.lane, report_root_name)

