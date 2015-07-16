import os
from math import ceil

from genologics.lims import *
from common import nsc, utilities, slurm

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


def run_dmx(process, n_threads, run_dir, input_dir, output_dir,
        no_lane_splitting, other_options):
    """Run bcl2fastq2 via srun."""

    args = ['--runfolder-dir', run_dir]
    if no_lane_splitting:
        args += ['--no-lane-splitting']
    args += ['--input-dir', input_dir]
    args += ['--output-dir', output_dir]
    args += get_thread_args(n_threads)
    if other_options:
        args += re.split(" +", other_options)

    log_path = utilities.logfile(process, nsc.CJU_DEMULTIPLEXING, "bcl2fastq2")
    with open(log_path, "w") as log:
        jobname = process.id + ".bcl2fastq2"
        rcode = slurm.srun_command(
                args, jobname, time="1-0", logfile=log_path,
                cpus_per_task=n_threads, mem="8G"
                )
    utilities.upload_file(process, nsc.BCL2FASTQ_LOG, log_path)

    return rcode == 0


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




def main(process_id):
    process = Process(nsc.lims, id=process_id)
    utilities.running(process, nsc.CJU_DEMULTIPLEXING)
    seq_process = utilities.get_sequencing_process(process)
    run_id = seq_process.udf['Run ID']
    source_run_dir = utilities.get_udf(
            process, nsc.SOURCE_RUN_DIR_UDF,
            os.path.join(nsc.PRIMARY_STORAGE, run_id)
            )
    dest_run_dir = utilities.get_udf(
            process, nsc.WORK_RUN_DIR_UDF,
            os.path.join(nsc.SECONDARY_STORAGE, run_id)
            )


    sample_sheet_data = download_sample_sheet(process)
    if sample_sheet_data:
        with open(os.path.join(dest_run_dir, "SampleSheet.csv")) as f:
            f.write(sample_sheet_data)
        sample_sheet_path = utilities.logfile(nsc.CJU_DEMULTIPLEXING, "SampleSheet", "csv")
        with open(sample_sheet_path) as f:
            f.write(sample_sheet_data)

    utilities.running(process, nsc.CJU_DEMULTIPLEXING, "Demultiplexing")

    threads = utilities.get_udf(process, nsc.THREADS_UDF, 1)
    default_no_lane_splitting = utilities.get_instrument(seq_process) == "nextseq"
    no_lane_splitting = utilities.get_udf(
            process, nsc.NO_LANE_SPLITTING_UDF, default_no_lane_splitting
            )
    other_options = utilities.get_udf(process, nsc.OTHER_OPTIONS_UDF, None)

    if run_dmx(
            process, n_threads, input_dir, output_dir, no_lane_splitting,
            other_options
            ):
        utilities.success_finish(process)
    else:
        utilities.fail(process, "bcl2fastq failure (see log)") 
        sys.exit(1)


main(sys.argv[1])

