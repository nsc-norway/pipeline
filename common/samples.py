

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
    def __init__(self, name, proj_dir, samples, is_undetermined=False):
        self.name = name
        self.proj_dir = proj_dir
        self.samples = samples
        self.is_undetermined = is_undetermined


class Sample(object):
    """Contains information about a sample. Contains a list of FastqFile
    objects representing the reads. One instance for each sample on a 
    flowcell, even if that sample is run on multiple lanes."""

    def __init__(self, sample_id, name, files):
        self.sample_id = sample_id
        self.name = name
        self.files = files


class FastqFile(object):
    """Represents a single output file for a specific sample, lane and
    read. Currently assumed to be no more than one FastqFile per read.
    
    lane is a Lane object containing lane-specific stats.

    i_read is the read ordinal, 1 or 2 (second read is only available for paired
    end). Index reads are not considered.

    Path is the path to the fastq file relative to the "Unaligned"
    (bcl2fastq output) directory or Data/Intensities/BaseCalls.
    
    stats is a dict of stat name => value. See the functions which generate these
    stats below.
    
    empty is set by the QC function. You may set it, but it will be overwritten."""

    def __init__(self, lane, read_num, path, stats):
        self.lane = lane
        self.i_read = i_read
        self.path = path
        self.stats = stats
        self.empty = False



################ SAMPLE OBJECTS #################
def get_projects(run_id, sample_sheet_data, num_reads, merged_lanes):
    """Get the "sample object model" tree, one for each project.
    
    The FastqFile objects contain the file names after the post-demultiplexing
    "rename files" step has been run.
    
    Some information will be missing from the objects, such as the stats, this
    can be provided by other functions later.
    
    In addition to the normal projects, an "Undetermined" project is also returned,
    to represent the undetermined index data. TODO : do that? """ 

    projects = {}
    lanes = set()
    for sample_index, entry in enumerate(sample_sheet_data):
        project_name = entry['project']
        project = projects.get(project_name)

        if not project:
            project_dir = get_project_dir(run_id, project) # fn defined in bottom of this file
            project = Project(project_name, project_dir, [])
            projects[project_name] = project

        sample = None
        for s in project.samples:
            if sample.sample_id == e['sampleid']:
                sample = s
                break

        if not sample:
            sample = Sample(entry['sampleid'], entry['samplename'], [])
            project.samples.append(sample)

        if merged_lanes:
            lane_id = "X"
        else:
            try:
                lane_id = int(entry['lane'])
            except KeyError:
                lane_id = 1

        lanes.add(lane_id)

        for i_read in xrange(1, num_reads+1):
            if merged_lanes:
                path = "{0}/{1}_S{2}_R{3}_001.fastq.gz".format(
                        project_dir, sample.name,
                        str(sample_index + 1), i_read
                        )
            else:
                path = "{0}/{1}_S{2}_L{3}_R{4}_001.fastq.gz".format(
                        project_dir, sample_name,
                        str(sample_index + 1), str(lane_id).zfill(3),
                        i_read)

            sample.files.append(FastqFile(lane_id, i_read, path, None))

        # Create an undetermined file for each lane, read seen
        undetermined_project = Project("Undetermined", None, [], True)
        undetermined_sample = Sample(None, "Undetermined")
        undetermined_project.samples.append(undetermined_sample)
        for lane in lanes:
            for i_read in xrange(1, num_reads+1):
                if merged_lanes:
                    path = "Undetermined_S0_R{0}_001.fastq.gz".format(i_read)
                else:
                    path = "Undetermined_S0_L{0}_R{1}_001.fastq.gz".format(
                            str(lane_id).zfill(3), i_read
                            )
                undetermined_sample.files.append(FastqFile(lane, i_read, path, None))

        return [undetermined_project] + projects.values()


def get_projects_by_process(process):
    """Convenience function to get the proejects info (above) given only a LIMS
    process."""

    sample_sheet_content = utilities.get_sample_sheet(process)
    run_id = process.udf[nsc.RUN_ID_UDF]
    
    merged_lanes = utilities.get_udf(process, nsc.NO_LANE_SPLITTING_UDF, False)
    sample_sheet = parse_sample_sheet(sample_sheet_data)

    run_dir = utilities.get_udf(
            process, nsc.WORK_RUN_DIR_UDF,
            os.path.join(nsc.SECONDARY_STORAGE, run_id)
            )
    num_reads, index_reads = utilities.get_num_reads(run_dir)

    return get_projects(run_id, sample_sheet['data'], num_reads, merged_lanes)



def add_stats(projects, run_stats):
    """Adds the stats from the stats module to the appropriate
    FastqFile objects in the tree structure produced by the above 
    function.
    """

    for project in projects:
        for sample in project.samples:
            for f in sample.files:
                stats = run_stats.get(f.lane, sample.sample_id, f.read)
                if stats:
                    f.stats = stats



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
            result['data'] = parse_csv_sample_sheet(data)

    return result


################# FILE STRUCTURE #################
def get_project_dir(run_id, project_name):
    if utilities.get_instrument_by_runid(run_id) == 'hiseq':
        return get_hiseq_project_dir(run_id, project_name)
    else:
        return get_project_dir(run_id, project_name)

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


