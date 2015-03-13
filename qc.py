# QC functions top level module file

# This module provides QC-related functions for all Illumina sequencer types.

import subprocess


def run_fastqc(fastq_dir, logfile=subprocess.STDOUT, max_threads=None):
    '''Run fastqc on a set of fastq files'''
    args = []
    if max_threads:
        args += ['--threads=', max_threads]
    rc = subprocess.call([nsc.FASTQC] + args, stderr=logfile, stdout=logfile)
    

def generate_report():
    pass


