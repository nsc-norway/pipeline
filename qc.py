# QC functions top level module file

# This module provides QC-related functions for all Illumina sequencer types.

import subprocess


def run_fastqc(files, logfile=subprocess.STDOUT, max_threads=None):
    '''Run fastqc on a set of fastq files'''
    args = []
    if max_threads:
        args += ['--threads=' + max_threads]
    args += files
    rc = subprocess.call([nsc.FASTQC] + args, stderr=logfile, stdout=logfile)
    


class Project(object):
    def __init__(self, project_dir, samples=[]):
        self.project_dir = project_dir
        self.samples = samples


class Sample(object):
    def __init__(self, paths):
        self.paths = paths


def qc_main(demultiplex_dir, projects_samples, data_reads, index_reads,
        threads = 1):
    '''demultiplex_dir is the location of the demultiplexed reads.

    projects_samples is a list of Project objects containing references
    to samples and files. This is a generalised specification of 
    the information in the sample sheet, valid for all Illumina
    instrument types.

    data_reads and index_reads are lists of integers representing
    the number of cycles in each read.
    '''
    all_fastq = [p for p in s.paths for s in pro.samples for pro in projects_samples]
    run_fastqc()

