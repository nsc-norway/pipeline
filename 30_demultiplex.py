import os
from math import ceil

from genologics.lims import *
from common import nsc, utilities, slurm, samples, taskmgr

TASK_NAME = "Demultiplexing"
TASK_DESCRIPTION = """Demultiplexing (calls bcl2fastq2). 
                    For advanced command-line options when running outside the
                    lims, just use bcl2fastq2 directly."""
TASK_ARGS = ['src_dir', 'work_dir', 'threads', 'sample_sheet']


def main(task):
    os.umask(007)

    task.running()
    run_id = task.run_id

    print "Demultiplexing process for LIMS process", process_id, ", run", run_id

    source_run_dir = task.src_dir
    input_dir = os.path.join(source_run_dir, "Data", "Intensities", "BaseCalls")
    dest_run_dir = task.work_dir
    print "Reading from", source_run_dir, "writing to", dest_run_dir

    threads = task.threads

    default_no_lane_splitting = utilities.get_instrument_by_runid(run_id) == "nextseq"

    # bcl2fastq2 options
    if task.process:
        no_lane_splitting = utilities.get_udf(
                process, nsc.NO_LANE_SPLITTING_UDF, default_no_lane_splitting
                )
        other_options = utilities.get_udf(process, nsc.OTHER_OPTIONS_UDF, None)
    else:
        # for non-lims mode, we don't provide many options, just the defaults
        no_lane_splitting = default_no_lane_splitting
        other_options = []

    # Sample sheet
    if task.process: # LIMS mode
        sample_sheet_content = task.sample_sheet_content
        demultiplex_sample_sheet_path = os.path.join(
                dest_run_dir,
                "DemultiplexingSampleSheet-" + process.id + ".csv"
                )
        with open(demultiplex_sample_sheet_path, 'wb') as f:
            f.write(sample_sheet_content)

    else: # command line mode
        demultiplex_sample_sheet_path = task.sample_sheet_path

    if run_dmx(
            process, n_threads, dest_run_dir, input_dir, no_lane_splitting,
            other_options
            ):
        task.success_finish()
    else:
        task.fail(process, "bcl2fastq failure (see log)") 


def get_thread_args(n_threads):
    # Computing number of threads: use the standard allocation 
    # logic for bcl2fastq 2.17, but based on n_threads instead of
    # the number of threads on the machine
    loading = 4
    writing = 4
    demultiplexing = int(ceil(n_threads * 0.2)) #  20 %
    processing = n_threads                      # 100 %
    return ['-r', str(loading), '-d', str(demultiplexing),
            '-p', str(processing), '-w', str(writing)]


def run_dmx(task, n_threads, run_dir, input_dir, sample_sheet_path,
        no_lane_splitting, other_options):
    """Run bcl2fastq2 via srun.
    
    Speciy run_dir as the destination directory and input_dir as the source
    directory containing BCLs, <source_run>/Data/Intensities/BaseCalls."""

    args = ['--runfolder-dir', run_dir]
    args += ['--sample-sheet', sample_sheet_path]
    if no_lane_splitting:
        args += ['--no-lane-splitting']
    args += ['--input-dir', input_dir]
    args += get_thread_args(n_threads)
    if other_options:
        args += re.split(" +", other_options)

    log_path = task.logfile("bcl2fastq2")
    jobname = "bcl2fastq2"
    if task.process:
        jobname = task.process.id + "." + jobname
    rcode = slurm.srun_command(
            args, jobname, time="1-0", logfile=log_path,
            cpus_per_task=n_threads, mem="8G"
            )
    utilities.upload_file(process, nsc.BCL2FASTQ_LOG, log_path)

    # Get the bcl2fastq version
    if task.process:
        log = open(log_path)
        for i in xrange(3):
            l = next(log)
            if l.startswith("bcl2fastq v"):
                process.udf[nsc.BCL2FASTQ_VERSION_UDF] = l.split(" ")[1].strip("\n")
                # No put(), will put later when calling success_finish() or fail()

    return rcode == 0



if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

