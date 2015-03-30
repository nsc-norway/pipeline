# QC functions top level module file

# This module provides QC-related functions for all Illumina sequencer types.

import subprocess


def run_fastqc(files, logfile=subprocess.STDOUT, output_dir=None, max_threads=None):
    '''Run fastqc on a set of fastq files'''
    args = []
    if max_threads:
        args += ['--threads=' + max_threads]
    if output_dir:
        args += ["--outdir=" + output_dir]
    args += files
    rc = subprocess.call([nsc.FASTQC] + args, stderr=logfile, stdout=logfile)
    

def move_fastqc_results(fastqc_dir, samples):
    for s in samples:
        sample_dir = os.path.join(fastqc_dir, "Sample_" + s.name)
        try:
            os.mkdir(sample_dir)
        except OSError:
            print "Warning: Writing into existing sample directory Sample_" + s.name

        


class Project(object):
    def __init__(self, project_name, samples=[]):
        self.project_name = project_name
        self.samples = samples


class Sample(object):
    def __init__(self, name, paths):
        self.name = name
        self.paths = paths


def qc_main(demultiplex_dir, projects_samples, data_reads, index_reads,
        run_id, software_versions, threads = 1):
    '''demultiplex_dir is the location of the demultiplexed reads.

    projects_samples is a list of Project objects containing references
    to samples and files. This is a generalised specification of 
    the information in the sample sheet, valid for all Illumina
    instrument types.

    data_reads and index_reads are lists of integers representing
    the number of cycles in each read.
    '''
    output_dir = os.path.join(demultiplex_dir, "inHouseDataProcessing")
    fastqc_dir = os.path.join(output_dir, "QualityControl")
    try:
        os.mkdir(output_dir) # Unaligned/inHouseDataProcessing/
    except OSError:
        pass
    try:
        os.mkdir(fastqc_dir) # Unaligned/inHouseDataProcessing/QualityControl
    except OSError:
        pass
    

    # First dump all fastqc output into QualityControl, them move thme 
    # in place later
    all_fastq = [p for p in s.paths for s in pro.samples for pro in projects_samples]

    if len(set(os.path.split(p)[1] for p in all_fastq)) < len(all_fastq):
        raise Exception("Not all fastq file names are unique! Can't deal with this, consider splitting into smaller jobs.")

    run_fastqc(all_fastq, output_dir=fastqc_dir, max_threads=threads) 
    move_fastqc_results(s for s in pro.samples for pro in projects_samples)


