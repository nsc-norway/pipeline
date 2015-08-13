# QC functions library module file

# This module provides QC-related functions for all Illumina sequencer types.

import subprocess
import re
import os
import shutil
import gzip
import operator
from multiprocessing import Pool
import nsc, utilities

template_dir = os.path.dirname(os.path.dirname(__file__)) + "/template"

# Model: Objects containing projects, samples and files.
# Represents a unified interface to the QC functionality, similar to the
# SampleInformation.txt BarcodeLaneStatistics.txt from the perl scripts.

class Project(object):
    """Project object.
    name: name with special characters replaced
    path: base name of project directory 
      path is relative to Unaligned for HiSeq, and relative to Data/Intensities/BaseCalls
      for MiSeq and NextSeq
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



############### FASTQC ################
def run_fastqc(files, demultiplex_dir, output_dir=None, max_threads=None):
    """Run fastqc on a set of fastq files"""
    args = ['--extract']
    if max_threads:
        args += ['--threads=' + str(max_threads)]
    if output_dir:
        args += ["--outdir=" + output_dir]
    args += files
    if len(files) > 0:
        print "Running fastqc on", len(files), "files"
        DEVNULL = open(os.devnull, 'wb') # discard output
        rc = subprocess.call([nsc.FASTQC] + args, cwd=demultiplex_dir)
    else:
        print "No files provided for fastqc"
    


def fastqc_dir(fp):
    """Get base name of fastqc directory given the name of the fastq file"""
    if not fp.endswith(".fastq.gz"):
        raise ValueError("Can only process fastq.gz files!")
    return re.sub(".fastq.gz$", "_fastqc", os.path.basename(fp))



def compute_md5(proj_dir, threads, files):
    md5data = utilities.check_output([nsc.MD5DEEP, "-rl", "-j" + str(threads)] + files,
            cwd=proj_dir)
    open(os.path.join(proj_dir, "md5sum.txt"), "w").write(md5data)

            

def qc_main(input_demultiplex_dir, projects, instrument_type, run_id,
        software_versions, threads = 1):
    """QC on demultiplexed data. Can be run per project, don't need
    access to all demultiplexed lanes.

    input_demultiplex_dir is the location of the demultiplexed reads,
    i.e., Unaligned.

    projects is a list of Project objects containing references
    to samples and files. See above for Project, Sample and 
    FastqFile classes. This is a generalised specification of 
    the information in the sample sheet, valid for all Illumina
    instrument types. It also contains some data for the results, 
    not just experiment setup.

    software_versions is a dict with (software name: version)
    software name: RTA, bcl2fastq
    """
    os.umask(007)

    # Unaligned
    demultiplex_dir = os.path.abspath(input_demultiplex_dir)
    # Unaligned/QualityControl
    quality_control_dir = os.path.join(demultiplex_dir, "QualityControl")
    # Unaligned/QualityControl/Delivery
    delivery_dir = quality_control_dir + "/Delivery"
    for d in [quality_control_dir, delivery_dir]:
        try:
            os.mkdir(d) 
        except OSError:
            pass

    print "Number of projects: ", len(projects)
    all_fastq = []
    non_empty_files = []

    for f in [f for pro in projects for s in pro.samples for f in s.files]:
        all_fastq.append(f.path)
        gzf = gzip.open(os.path.join(demultiplex_dir, f.path))
        f.empty = len(gzf.read(1)) == 0
        gzf.close()
        if not f.empty:
            non_empty_files.append(f.path)

    if len(set(os.path.basename(f) for f in all_fastq)) < len(all_fastq):
        raise RuntimeError("Not all fastq file names are unique! Can't deal with this, consider splitting into smaller jobs.")

    # Run FastQC
    # First output all fastqc results into QualityControl, them move them
    # in place later
    run_fastqc(non_empty_files, demultiplex_dir, output_dir=quality_control_dir, max_threads=threads) 

    samples = [sam for pro in projects for sam in pro.samples]
    for s in samples:
       move_fastqc_results(quality_control_dir, s)
       # Get number of sequences. For (Mi|Next)Seq this is the only way to 
       # get this stat, for HiSeq this acts as a cross check.
       update_stats_fastqc(quality_control_dir, s)

    # Generate PDF reports in parallel
    template = open(template_dir + "/reportTemplate_indLane_v4.tex").read()
    arg_pack = [demultiplex_dir, quality_control_dir, run_id, software_versions, template]
    pool = Pool(int(threads))
    # Run one task for each fastq file, giving a sample reference and FastqFile as argument 
    # as well as the ones given above. Debug note: change pool.map to map for better errors.
    pool.map(generate_report_for_customer, [tuple(arg_pack + [s,f]) for s in samples for f in s.files if not f.empty]) 
    
    # Generate md5sums for projects
    for p in projects:
        if not p.is_undetermined:
            if p.proj_dir:
                paths = [
                        re.sub(r"^{0}".format(re.escape(p.proj_dir)), ".", f.path)
                            for s in p.samples for f in s.files
                        ]
                # PDFs are in same directory as fastq file, with a special name given by 
                # qc_pdf_name
                pdf_paths = [
                        re.sub(r"^{0}".format(re.escape(p.proj_dir)), ".",
                            os.path.join(os.path.dirname(f.path), qc_pdf_name(run_id, f)))
                            for s in p.samples for f in s.files
                        ]
                compute_md5(os.path.join(demultiplex_dir, p.proj_dir), threads, paths + pdf_paths)
            else: # Project files are in root of demultiplexing dir
                paths = [f.path for s in p.samples for f in s.files]
                pdf_paths = [re.sub(r".fastq.gz$", ".qc.pdf", path) for path in paths]
                compute_md5(demultiplex_dir, threads, ["./" + path for path in paths + pdf_paths])

    # Generate internal reports
    generate_internal_html_report(quality_control_dir, samples)
    
    # For email to customers
    for project in projects:
        if project.name != "Undetermined_indices":
            fname = delivery_dir + "/Email_for_" + project.name + ".xls"
            write_sample_info_table(fname, run_id, project)


    # For internal bookkeeping -- This will not be needed when we move to Clarity LIMS
    # exclusively, but we need to figure out how to get all the stats into LIMS for all 
    # sequencer types.
    fname = delivery_dir + "/Table_for_GA_runs_" + run_id + ".xls"
    write_internal_sample_table(fname, run_id, projects)

    # Summary email for NSC staff
    fname = delivery_dir + "/Summary_email_for_NSC_" + run_id + ".xls"
    write_summary_email(fname, run_id, projects, instrument_type=='hiseq')


