#!/bin/env python

# Demultiplexing script for HiSeq -- non-LIMSified version

# This script is designed to be run with sbatch directly. 
# The following options should be specified for sbatch on 
# the command line: 
# --mem
# --nthreads
# Demultiplexing options can be given after the script name.
# These are passed to configureBclToFastq.pl




import sys, os
from genologics.lims import *
import nsc
import utilities


def get_config(process):
    try:
        cfg = object()
        cfg.bases_mask = process.udf[nsc.BASES_MASK_UDF]
        cfg.n_threads = process.udf[nsc.THREADS_UDF]
        cfg.mismatches = process.udf[nsc.MISMATCHES_UDF]
        cfg.run_dir = process.udf[nsc.SOURCE_RUN_DIR_UDF]
        cfg.dest_dir = process.udf[nsc.DEST_FASTQ_DIR_UDF]
    except KeyError:
        cfg = None

    return cfg


def download_sample_sheet(process, save_dir):
    sample_sheet = None
    for o in process.all_outputs(unique=True):
        if o.output_type == "ResultFile" and o.name == "SampleSheet csv":
            if len(o.files) == 1:
                sample_sheet = o.files[0].download()

    if sample_sheet:
        name = "SampleSheet-" + process.id + ".csv"
        file(os.path.join(save_dir, name), 'w').write(sample_sheet)
        return name
    else:
        return False


def run_demultiplexing(process, ssheet, bases_mask, n_threads, mismatches, start_dir, dest_run_dir):
    os.chdir(start_dir)
    cfg_log_name = os.path.join(nsc.LOG_DIR, "configureBclToFastq-" + process.id + ".log")
    log = open(cfg_log_name, "w")
    
    args = ['--mismatches', str(mismatches)]
    args += ['--fastq-cluster-count', 0]
    args += ['--sample-sheet', ssheet]
    args += ['--use-bases-mask', bases_mask]
    args += ['--output-dir', dest_run_dir]

    # configureBclToFastq.pl
    rcode = subprocess.call([nsc.CONFIGURE_BCL_TO_FASTQ] + args, stdout=log, stderr=log)

    log.close()
    utilities.upload_file(process, cfg_log_name)

    if rcode == 0:
        os.chdir(dest_run_dir)
        args = ['-j' + str(n_threads)]
        make_log_file = os.path.join(nsc.LOG_DIR, "make_" + process_id + ".log")
        log = open(make_log_file, "w")

        rcode = subprocess.call([nsc.MAKE] + args)
        log.close()

        utilities.upload_file(process, make_log_file)

        if rcode == 0:
            return True
        else:
            utilities.fail(process, "Make failure (see log)")
    else:
        utilities.fail(process, "configureBclToFastq failure (see log)")
    
    return False


def main(process_id):
    process = Process(nsc.lims, id=process_id)

    utilities.running(process)

    cfg = get_config(process)

    success = False
    if cfg:
        start_dir = os.path.join(cfg.run_dir, "Data", "Intensities", "BaseCalls")
    
        ssheet = download_sample_sheet(process, start_dir)
    
        if ssheet:
            success = run_demultiplexing(process, ssheet, cfg.bases_mask,
                    cfg.n_threads, cfg.mismatches, start_dir, cfg.dest_dir)
            
        else:
            utilities.fail(process, "Can't get the sample sheet")

    else:
        utilities.fail(process, "Missing configuration information, can't demultiplex")
        
    
    if success:
        utilities.success_finish(process)
    # If failing, should have already notified of failure, just quit
    else:
        if not process.udf[nsc.JOB_STATUS_UDF].startswith('Fail'):
            utilities.fail(process, "Unknown failure")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        main(sys.argv[1])
    else:
        print "use: demultiplex-hiseq.py <process-id>"
        sys.exit(1)


