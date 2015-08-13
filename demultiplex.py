import os
from math import ceil

from genologics.lims import *
from common import nsc, utilities, slurm

def main(process_id):
    os.umask(007)
    process = Process(nsc.lims, id=process_id)
    utilities.running(process, nsc.CJU_DEMULTIPLEXING)

    run_id = process.udf[nsc.RUN_ID_UDF]

    print "Demultiplexing process for LIMS process", process_id, ", NextSeq run", run_id

    source_run_dir = utilities.get_udf(
            process, nsc.SOURCE_RUN_DIR_UDF,
            os.path.join(nsc.PRIMARY_STORAGE, run_id)
            )
    input_dir = os.path.join(source_run_dir, "Data", "Intensities", "BaseCalls")
    dest_run_dir = utilities.get_udf(
            process, nsc.WORK_RUN_DIR_UDF,
            os.path.join(nsc.SECONDARY_STORAGE, run_id)
            )
    print "Reading from", source_run_dir, "writing to", dest_run_dir

    sample_sheet_content = utilities.get_sample_sheet(process)
    if sample_sheet_content:
        with open(os.path.join(dest_run_dir, "DemultiplexingSampleSheet.csv")) as f:
            f.write(sample_sheet_content)

    utilities.running(process, nsc.CJU_DEMULTIPLEXING, "Demultiplexing")

    threads = utilities.get_udf(process, nsc.THREADS_UDF, 1)
    default_no_lane_splitting = utilities.get_instrument_by_runid(run_id) == "nextseq"
    no_lane_splitting = utilities.get_udf(
            process, nsc.NO_LANE_SPLITTING_UDF, default_no_lane_splitting
            )
    other_options = utilities.get_udf(process, nsc.OTHER_OPTIONS_UDF, None)

    if run_dmx(
            process, n_threads, dest_run_dir, input_dir, no_lane_splitting,
            other_options
            ):
        utilities.success_finish(process)
        return True
    else:
        utilities.fail(process, "bcl2fastq failure (see log)") 
        return False


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


def run_dmx(process, n_threads, run_dir, input_dir,
        no_lane_splitting, other_options):
    """Run bcl2fastq2 via srun.
    
    Speciy run_dir as the destination directory and input_dir as the source
    directory containing BCLs, <source_run>/Data/Intensities/BaseCalls."""

    args = ['--runfolder-dir', run_dir]
    sample_sheet_path = os.path.join(run_dir, 'DemultiplexingSampleSheet.csv')
    args += ['--sample-sheet', sample_sheet_path]
    if no_lane_splitting:
        args += ['--no-lane-splitting']
    args += ['--input-dir', input_dir]
    args += get_thread_args(n_threads)
    if other_options:
        args += re.split(" +", other_options)

    log_path = utilities.logfile(process, nsc.CJU_DEMULTIPLEXING, "bcl2fastq2")
    jobname = process.id + ".bcl2fastq2"
    rcode = slurm.srun_command(
            args, jobname, time="1-0", logfile=log_path,
            cpus_per_task=n_threads, mem="8G"
            )
    utilities.upload_file(process, nsc.BCL2FASTQ_LOG, log_path)

    return rcode == 0



if __name__ == "__main__":
    if len(sys.argv) >= 2:
        with utilities.error_reporter():
            ok = main(sys.argv[1])
            sys.exit(0 if ok else 1)
    else:
        print "use: demultiplex.py <process-id>"
        sys.exit(1)


