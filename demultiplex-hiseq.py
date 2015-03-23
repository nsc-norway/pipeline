# Demultiplexing script for HiSeq

# This is the primary demultiplexing job. It is contolled by the setup-
# hiseq-demultiplexing script, through the use of UDFs on the process in the
# LIMS.

# Its primary data processing functionality is handled by:
# configureBclToFastq.pl
# make

# Functional overview:
# - Get options for demultiplexing 
# - Set running / error flag in UDF
# - Run demultiplexing
# - Upload result files and set UDFs on samples
# - Set job finished status
# - Finsih demultiplexing step if using automatic processing

import sys, os, re
import subprocess
from genologics.lims import *
import nsc
import utilities
import demultiplex

class Config:
    pass

def get_config(process):
    '''Configuration is stored in UDFs on the demultiplexing process. This
    function loads them into a generic object.'''

    try:
        cfg = Config()
        cfg.bases_mask = process.udf[nsc.BASES_MASK_UDF]
        cfg.n_threads = process.udf[nsc.THREADS_UDF]
        cfg.mismatches = process.udf[nsc.MISMATCHES_UDF]
        cfg.run_dir = process.udf[nsc.SOURCE_RUN_DIR_UDF]
        cfg.dest_dir = process.udf[nsc.DEST_FASTQ_DIR_UDF]
        cfg.other_options = process.udf[nsc.OTHER_OPTIONS_UDF]
    except KeyError:
        cfg = None

    return cfg


def download_sample_sheet(process, save_dir):
    '''Downloads the demultiplexing process's sample sheet, which contains only
    samples for the requested project (written by setup-demultiplex-hiseq.py).'''

    sample_sheet = None
    for o in process.all_outputs(unique=True):
        if o.output_type == "ResultFile" and o.name == "SampleSheet csv":
            if len(o.files) == 1:
                sample_sheet = o.files[0].download()

    if sample_sheet:
        if process.id == "":
            raise ValueError("Process ID is an empty string")
        name = "SampleSheet-" + process.id + ".csv"
        file(os.path.join(save_dir, name), 'w').write(sample_sheet)
        return name
    else:
        return False


def run_demultiplexing(process, ssheet, bases_mask, n_threads, mismatches,
        start_dir, dest_run_dir, other_options):
    '''First calls the configureFastqToBcl.py, then calls make in the fastq file directory.'''

    os.chdir(start_dir)
    cfg_log_name = os.path.join(nsc.LOG_DIR, "configureBclToFastq-" + process.id + ".log")
    log = open(cfg_log_name, "w")
    
    args = ['--mismatches', str(mismatches)]
    args += ['--fastq-cluster-count', "0"]
    args += ['--sample-sheet', ssheet]
    args += ['--use-bases-mask', bases_mask]
    args += ['--output-dir', dest_run_dir]
    args += ['--input-dir', start_dir]
    if other_options:
        args += re.split(" *", other_options)

    # configureBclToFastq.pl
    rcode = subprocess.call([nsc.CONFIGURE_BCL_TO_FASTQ] + args, stdout=log, stderr=log)

    log.close()
    utilities.upload_file(process, nsc.CONFIGURE_LOG, cfg_log_name)

    if rcode == 0:
        os.chdir(dest_run_dir)
        args = ['-j' + str(n_threads)]
        make_log_file = os.path.join(nsc.LOG_DIR, "make_" + process.id + ".log")
        log = open(make_log_file, "w")

        rcode = subprocess.call([nsc.MAKE] + args, stdout=log, stderr=log)
        log.close()

        utilities.upload_file(process, nsc.MAKE_LOG, make_log_file)

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
            process_ok = run_demultiplexing(process, ssheet, cfg.bases_mask,
                    cfg.n_threads, cfg.mismatches, start_dir, cfg.dest_dir,
                    cfg.other_options)
            if process_ok:
                try:
                    success = demultiplex.populate_results(process, fastq_dir)
                except (IOError,KeyError):
                    success = False

                if not success:
                    utilities.fail(process, "Failed to set UDFs")
            else: # Processing (make, etc)
                utilities.fail(process, "Demultiplexing process exited with an error status")
            
        else: # Sample sheet
            utilities.fail(process, "Can't get the sample sheet")

    else: # cfg
        utilities.fail(process, "Missing configuration information, can't demultiplex")
        
    
    if success:
        utilities.success_finish(process)
    # If failing, should have already notified of failure, just quit
    else:
        if not process.udf[nsc.JOB_STATUS_UDF].startswith('Fail'):
            utilities.fail(process, "Unknown failure")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        try:
            main(sys.argv[1])
        except:
            process = Process(nsc.lims, id = sys.argv[1])
            utilities.fail(process, "Unexpected: " + str(sys.exc_info()[1]))
            raise sys.exc_info()[1]
    else:
        print "use: demultiplex-hiseq.py <process-id>"
        sys.exit(1)


