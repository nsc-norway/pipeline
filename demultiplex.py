import os
from math import ceil

from genologics.lims import *
from common import nsc

def get_thread_args(n_threads):
    # Computing number of threads: use the standard allocation 
    # logic for bcl2fastq2, but limit ourselves to n_threads
    # (default is to use all cores on system)
    base = n_threads * 1.5
    loading = int(max(1, base * 0.1))
    # No more write threads than 1 per sample
    writing = 4
    processing = n_threads
    demultiplexing = max(int(ceil(0.2 * n_threads)), 1)
    return ['-r', str(loading), '-d', str(demultiplexing),
            '-p', str(processing), '-w', str(writing)]


def get_sample_sheet(process):
    """Downloads the demultiplexing process's sample sheet and returns
    the data."""

    sample_sheet = None
    for o in process.all_outputs(unique=True):
        if o.output_type == "ResultFile" and o.name == "SampleSheet csv":
            if len(o.files) == 1:
                return o.files[0].download()
    else:
        return None



def make_log_dir(run_dir, process_id):
    demultiplex_logs = os.path.join(run_dir, "DemultiplexLogs")
    try:
        os.mkdir(demultiplex_logs)
    except OSError:
        pass
    log_dir = demultiplex_logs + process_id
    try:
        os.mkdir(log_dir)
    except OSError:
        pass
    return log_dir

def main(process_id):
    process = Process(nsc.lims, id=process_id)
    seq_process = utilities.get_sequencing_process(process)
    run_id = seq_process.udf['Run ID']
    try:
        run_dir = process.udf[WORK_RUN_DIR_UDF]
    except KeyError:
        run_dir = os.path.join(nsc.SECONDARY_STORAGE, runid)

    log_dir = make_log_dir(run_dir, process_id)

    sample_sheet_data = download_sample_sheet(process)
    if sample_sheet_data:
        with open(os.path.join(run_dir, "SampleSheet.csv")) as f:
            f.write(sample_sheet_data)
        with open(os.path.join(log_dir, "SampleSheet.csv")) as f:
            f.write(sample_sheet_data)




main(sys.argv[1])

