import os
import glob
import re
import gzip
from collections import Counter

from genologics.lims import *

# Local imports
from . import nsc
from . import utilities


# Sample object model
# Objects containing projects, samples and files.

class Project(object):
    """Project object.
    name: name with special characters replaced
    proj_dir: base name of project directory relative to Data/Intensities/BaseCalls
    samples: list of samples
    """
    def __init__(self, name, proj_dir, samples, is_undetermined=False):
        self.name = name
        self.proj_dir = proj_dir
        self.samples = samples
        self.is_undetermined = is_undetermined


class Sample(object):
    """Contains information about a sample. Contains a list of FastqFile
    objects representing the reads. One instance for each sample on a 
    flowcell, even if that sample is run on multiple lanes.
    
    sample_index is the 1-based index of the entry in the sample sheet,
    counting only unique samples
    
    """

    def __init__(self, sample_index, sample_id, name, sample_dir, files, description=None):
        self.sample_index = sample_index # NOTE: This is not the index sequence, but the position in sample sheet
        self.sample_id = sample_id
        self.name = name
        self.sample_dir = sample_dir
        self.files = files
        self.description = description

    @property
    def limsid(self):
        if self.description: return self.description
        else: return self.sample_id


class FastqFile(object):
    """Represents a single output file for a specific sample, lane and
    read. Currently assumed to be no more than one FastqFile per read.

    lane is an integer representing the lane, or "X" for merged lanes

    i_read is the read ordinal, 1 or 2 (second read is only available for paired
    end). Index reads are given STRING values I1 and I2. These are only 
    considered if add_index_read_files is called, otherwise only data reads 1
    and 2 are present.

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
def get_projects(run_id, sample_sheet_data, num_reads, merged_lanes, expand_lanes=[1],
        experiment_name=None, only_process_lanes=None):
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
    experiment_name     - Used for project name when project is not available
    only_process_lanes  - Only process these lanes. List of int. Works with both
                          sample-sheet lanes (as on HiSeq) and expanded lanes (as
                          on NextSeq).  However I don't see the need to limit
                          lanes on NS, so it will not be thoroughly tested.
                          Empty list means process all lanes.
    """ 

    projects = {}
    known_samples = {}
    lanes = set()
    not_multiplexed_lanes = set()
    instrument = utilities.get_instrument_by_runid(run_id)
    sample_index = 1
    only_process_lanes_set = set(only_process_lanes or [])
    for entry in sample_sheet_data:


        # First determine lanes for this entry, and possibly skip the 
        # whole entry
        if merged_lanes:
            file_lanes = set("X")
        elif 'lane' in entry:
            file_lanes =  set( (int(entry['lane']),) )
        else:
            file_lanes = set(expand_lanes)

        if only_process_lanes_set:
            file_lanes &= only_process_lanes_set

        project_name = entry.get('project') or entry.get('sampleproject')
        if not project_name:
            raise ValueError("Project name missing in sample sheet, but is required by the QC scripts")
        project_name = utilities.strip_chars(project_name)
        project = projects.get(project_name)
        if not project:
            project_dir = get_project_dir(run_id, project_name) # fn defined in bottom of this file
            project = Project(project_name, project_dir, [])
            if file_lanes:
                projects[project_name] = project # Don't add project if in ignored lane

        sample = known_samples.get((project_name, entry['sampleid']))
        if not sample:
            sample_name = entry['samplename']
            if sample_name == "":
                # MiSeq only uses Sample ID (at least for non-LIMS sample sheet)
                sample_name = entry['sampleid']
            sample_name = utilities.strip_chars(sample_name)
            sample_dir = get_sample_dir(instrument, sample_name)
            sample = Sample(sample_index, entry['sampleid'], sample_name, sample_dir, [], entry.get('description'))
            sample_index += 1
            known_samples[(project.name, sample.sample_id)] = sample
            if file_lanes: # Don't add sample if in ignored lane
                project.samples.append(sample)


        for lane_id in file_lanes:
            lanes.add(lane_id)

            # Keep track of non-indexed lanes. Non-indexed lanes don't have Undetermined
            if not entry.get('index') and not entry.get('index2'):
                not_multiplexed_lanes.add(lane_id)

            path = ""
            if project.proj_dir:
                path = project.proj_dir + "/"
            if sample.sample_dir:
                path += sample.sample_dir + "/"

            for i_read in range(1, num_reads+1):

                fastq_name = get_fastq_name(
                        instrument,
                        sample.name,
                        sample.sample_index,
                        entry.get('index'),
                        entry.get('index2'),
                        lane_id,
                        i_read,
                        run_id,
                        merged_lanes
                        )

                # path contains trailing slash
                fastq_path = path + fastq_name

                index1 = entry.get("index")
                index2 = entry.get("index2")
                if index1 and not index2:
                    index_sequence = index1
                elif index2 and not index1:
                    index_sequence = index2
                elif index2 and index2:
                    index_sequence = index1 + "-" + index2
                else:
                    index_sequence = ""
                
                # If there is a sample with the same SampleID as the current line, and also the same
                # lane ID, then we don't add it. This happens when each sample uses multiple indexes,
                # but get written to one file.
                if not any(f.lane == lane_id and f.i_read == i_read for f in sample.files):
                    sample.files.append(FastqFile(lane_id, i_read, fastq_name, fastq_path, index_sequence, None))
                # Stats can be added in later

    # Create an undetermined file for each lane, read seen
    undetermined_project = Project(None, None, [], True)
    undetermined_sample = Sample(0, None, None, None, [], None)
    undetermined_project.samples.append(undetermined_sample)
    for lane in lanes:
        if lane in not_multiplexed_lanes:
            continue
        for i_read in range(1, num_reads+1):
            if merged_lanes:
                path = "Undetermined_S0_R{0}_001.fastq.gz".format(i_read)
            else:
                path = "Undetermined_S0_L{0}_R{1}_001.fastq.gz".format(
                        str(lane).zfill(3), i_read
                        )
            undetermined_sample.files.append(FastqFile(lane, i_read, path, path, None, None))

    return [undetermined_project] + list(projects.values())


def check_files_merged_lanes(run_dir):
    basecalls_dir = os.path.join(run_dir, "Data", "Intensities", "BaseCalls")
    unmerged_exists = len(glob.glob(basecalls_dir + "/Undetermined_S0_L*_R1_001.fastq.gz")) > 0
    merged_exists = os.path.exists(basecalls_dir + "/Undetermined_S0_R1_001.fastq.gz")
    if not unmerged_exists and not merged_exists:
        unmerged_exists = (len(glob.glob(basecalls_dir + "/*/*_S1_L*_R1_001.fastq.gz")) > 0 or
                           len(glob.glob(basecalls_dir + "/*/*/*_S1_L*_R1_001.fastq.gz")) > 0)
        merged_exists = (len(glob.glob(basecalls_dir + "/*/*_S1_R1_001.fastq.gz")) > 0 or
                        len(glob.glob(basecalls_dir + "/*/*/*_S1_R1_001.fastq.gz")) > 0)
    if merged_exists and not unmerged_exists:
        return True
    elif unmerged_exists and not merged_exists:
        return False
    else:
        raise ValueError("Unable to determine if lanes were merged (no-lane-splitting option)."
               "Make sure there is at least one FASTQ file.")

def get_lane_numbers_from_fastq_files(run_dir):
    """This function identifies the lane numbers. Useful for the "expand_lanes" option
    used when the sample sheet does not have lane number. This assumes that the lanes
    """

    basecalls_dir = os.path.join(run_dir, "Data", "Intensities", "BaseCalls")
    files = glob.glob(basecalls_dir + "Undetermined_S0_L00*_R1_001.fastq.gz")
    files += glob.glob(basecalls_dir + "/*/*_S1_L00*_R1_001.fastq.gz")
    files += glob.glob(basecalls_dir + "/*/*/*_S1_L00*_R1_001.fastq.gz")
    return set(int(re.search(r"_L00(\d)_", os.path.basename(file)).group(1)) for file in files)


def add_stats(projects, run_stats):
    """Adds the stats from the stats module to the appropriate
    FastqFile objects in the tree structure produced by the above 
    function.
    """
    for project in projects:
        for sample in project.samples:
            for f in sample.files:
                stats = run_stats.get((f.lane, sample.sample_id, f.i_read))
                if stats:
                    f.stats = stats


def flag_empty_files(projects, run_dir):
    basecalls_dir = os.path.join(run_dir, "Data", "Intensities", "BaseCalls")
    for p in projects:
        for s in p.samples:
            for f in s.files:
                full_path = os.path.join(basecalls_dir, f.path)
                f.empty = not os.path.exists(full_path)


def add_index_read_files(projects, run_dir, force=False):
    """Add files for Index read 1 and 2 to the projects data structure, if they
    exist. The files are created if the option --create-fastq-for-index-reads
    is give to bcl2fastq."""
    basecalls_dir = os.path.join(run_dir, "Data", "Intensities", "BaseCalls")
    for p in projects:
        for s in p.samples:
            for f in s.files:
                if f.i_read == 1:
                    for i_index_read in [1,2]:
                        index_read_path = re.sub(r"R1_001.fastq.gz$",
                                "I{}_001.fastq.gz".format(i_index_read), f.path)
                        full_path = os.path.join(basecalls_dir, index_read_path)
                        if force or os.path.exists(full_path):
                            s.files.append(FastqFile(f.lane, "I{}".format(i_index_read),
                                os.path.basename(index_read_path), index_read_path,
                                f.index_sequence, None))


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

    if not sample_sheet: raise ValueError("No sample sheet provided (Demultiplexing Sample Sheet)")
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
    instrument = utilities.get_instrument_by_runid(run_id)
    if instrument.startswith('hiseq') or instrument == "novaseq":
        return get_dual_fc_instrument_project_dir(run_id, project_name)
    else:
        return get_ne_mi_project_dir(run_id, project_name)

def get_dual_fc_instrument_project_dir(run_id, project_name):
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
    if instrument in ['hiseq', 'hiseq4k', 'hiseqx', 'novaseq']:
        return "Sample_" + sample_name
    else:
        return None


def get_fastq_name(instrument, sample_name, sample_index,
        index1, index2, lane_id, i_read, run_id, merged_lanes):
    """The file name we want depends on the instument type, for consistency with older
    deliveries.
    
    HiSeq 2500: Use CASAVA naming scheme.

    Others: Use bcl2fastq v2 naming scheme. 

    CEES site: Using a naming scheme which includes the flowcell ID.
    """
    
    # Single / dual index string
    if index1:
        index_seq = index1
    else:
        index_seq = "NoIndex"
    if index2:
        index_seq += "-" + index2

    # All parameters used for formatting
    parameters = {
            "sample_name":sample_name,
            "sample_index":sample_index,
            "index_seq":index_seq,
            "lane_id": lane_id,
            "i_read":i_read,
        }
    if nsc.SITE and nsc.SITE.startswith("cees"):
        # Format for CEES site
        parameters['fcid'] = re.search(r"_[AB]([A-Z0-9]+)$", run_id).group(1)
        name = "{fcid}_{sample_name}_{index_seq}_L{lane_id:03}_R{i_read}_001.fastq.gz".format(**parameters)
    elif instrument == "hiseq":
        name = "{sample_name}_{index_seq}_L{lane_id:03}_R{i_read}_001.fastq.gz".format(**parameters)
    else:
        if merged_lanes:
            name = "{sample_name}_S{sample_index}_R{i_read}_001.fastq.gz".format(**parameters)
        else:
            name = "{sample_name}_S{sample_index}_L{lane_id:03}_R{i_read}_001.fastq.gz".format(**parameters)

    return utilities.strip_chars(name)


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
    return utilities.strip_chars(name)


def get_sample_qc_dir(project, sample):
    """Get the directory of the QC files for a sample."""
    if project.name:
        project_name = project.name
        sample_name = sample.name
    else:
        project_name = "Undetermined"
        sample_name = "Undetermined"
    return os.path.join(project_name, "Sample_" + sample_name)


def get_fastqc_dir(project, sample, fastqfile):
    """Get the directory in which the fastqc results are stored
    (after moving it)."""

    fqc_name = re.sub(r".fastq.gz$", "_fastqc", fastqfile.filename)
    return os.path.join(get_sample_qc_dir(project, sample), fqc_name)


def get_fastdup_path(project, sample, fastqfile):
    """Get the directory in which the fastqc results are stored
    (after moving it)."""

    fd_name = re.sub(r".fastq.gz$", "_fastdup.txt", fastqfile.filename)
    return os.path.join(get_sample_qc_dir(project, sample), fd_name)


def qc_pdf_name(run_id, fastq):
    """Get QC report name for a given FastqFile object"""
    report_root_name = re.sub(".fastq.gz$", ".qc", os.path.basename(fastq.path))
    miseq = utilities.get_instrument_by_runid(run_id) == "miseq"
    if fastq.lane == "X" or miseq: # Merged lanes or single-lane instrument
        return "{0}.{1}.pdf".format(run_id, report_root_name)
    else:
        return "{0}.{1}.{2}.pdf".format(run_id, fastq.lane, report_root_name)

