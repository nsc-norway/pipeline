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


def run_demultiplexing(process_id, ssheet, bases_mask, n_threads, mismatches, start_dir, dest_run_dir):
    os.chdir(start_dir)
    log = open("configureBclToFastq-" + process_id + ".log", "w")
    
    args = ['--mismatches', str(mismatches)]
    args += ['--fastq-cluster-count', 0]
    args += ['--sample-sheet', ssheet]
    args += ['--use-bases-mask', bases_mask]
    args += ['--output-dir', dest_run_dir]

    # configureBclToFastq.pl
    rcode = subprocess.check_call([nsc.CONFIGURE_BCL_TO_FASTQ] + args, stdout=log, stderr=log)
    log.close()

    utilities.upload_file()


    if rcode == 0:
        os.chdir(dest_run_dir)
        args = ['-j' + str(n_threads)]
        log = open("make_" + process_id + ".log", "w")


    

    

    return True


def main(process_id):
    process = Process(nsc.lims, id=process_id)
    cfg = get_config(process)

    if cfg:
        start_dir = os.path.join(cfg.run_dir, "Data", "Intensities", "BaseCalls")
    
        ssheet = download_sample_sheet(process, start_dir)
    
        if ssheet:
            status = run_demultiplexing(process_id, ssheet, cfg.bases_mask,
                    cfg.n_threads, cfg.mismatches, start_dir, cfg.dest_dir)

            
        else:
            utilities.fail(process, "Can't get the sample sheet")

    else:
        utilities.fail(process, "Missing configuration information, can't demultiplex")
        




if __name__ == "__main__":
    if len(sys.argv) >= 2:
        main(sys.argv[1])
    else:
        print "use: demultiplex-hiseq.py <process-id>"
        sys.exit(1)


