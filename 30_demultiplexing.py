import os
import re
from math import ceil

from genologics.lims import *
from common import nsc, utilities, remote, samples, taskmgr

TASK_NAME = "30. Demultiplexing"
TASK_DESCRIPTION = """Demultiplexing (calls bcl2fastq2). 
                    For advanced command-line options when running outside the
                    lims, just use bcl2fastq2 directly."""
TASK_ARGS = ['src_dir', 'work_dir', 'threads', 'sample_sheet', 'lanes']


def main(task):
    task.add_argument(
                      '--extra-options',
                      default=None,
                      help="Extra options to give to bcl2fastq."
                      )
    task.running()
    run_id = task.run_id

    if task.process:
        print "Demultiplexing process for LIMS process", task.process.id, ", run", run_id

    source_run_dir = task.src_dir
    dest_run_dir = task.work_dir
    print "Reading from", source_run_dir, "writing to", dest_run_dir

    threads = task.threads

    instrument = utilities.get_instrument_by_runid(run_id)
    default_no_lane_splitting =  instrument == "nextseq"

    # bcl2fastq2 options
    if task.process:
        no_lane_splitting = utilities.get_udf(
                task.process, nsc.NO_LANE_SPLITTING_UDF, default_no_lane_splitting
                )
        other_options = utilities.get_udf(task.process, nsc.OTHER_OPTIONS_UDF, None)
    else:
        # for non-lims mode, we don't provide many options, just the defaults
        no_lane_splitting = default_no_lane_splitting
        other_options = task.args.extra_options

    # Sample sheet
    if task.process: # LIMS mode
        sample_sheet_content = task.sample_sheet_content
        demultiplex_sample_sheet_path = os.path.join(
                dest_run_dir,
                "DemultiplexingSampleSheet-" + task.process.id + ".csv"
                )
        with open(demultiplex_sample_sheet_path, 'wb') as f:
            f.write(sample_sheet_content)

    else: # command line mode
        demultiplex_sample_sheet_path = task.sample_sheet_path

    output_dir = os.path.join(dest_run_dir, "Data", "Intensities", "BaseCalls")
    if run_dmx(
            task, threads, source_run_dir, output_dir, demultiplex_sample_sheet_path,
            no_lane_splitting, other_options, run_id
            ):
        task.success_finish()
    else:
        task.fail("bcl2fastq exited with an error status") 


def get_thread_args(n_threads):
    # Computing number of threads: use the standard allocation 
    # logic for bcl2fastq 2.17, but based on n_threads instead of
    # the number of threads on the machine
    loading = 4 + n_threads//4
    writing = 4 + n_threads//10
    demultiplexing = int(ceil(n_threads * 0.7)) #  70 %
    processing = int(ceil(n_threads * 0.8))     #  80 %
    return ['-r', str(loading), '-d', str(demultiplexing),
            '-p', str(processing), '-w', str(writing)]


def run_dmx(task, n_threads, run_dir, output_dir, sample_sheet_path,
        no_lane_splitting, other_options, comment):
    """Run bcl2fastq2 via srun.
    
    Speciy run_dir as the destination directory and input_dir as the source
    directory containing BCLs, <source_run>/Data/Intensities/BaseCalls."""

    args = [nsc.BCL2FASTQ2, '--runfolder-dir', run_dir]
    args += ['--sample-sheet', sample_sheet_path]
    if no_lane_splitting:
        args += ['--no-lane-splitting']
    args += ['--output-dir', output_dir]
    if task.suffix:
        args += ['--reports-dir', os.path.join(output_dir, "Reports" + task.suffix)]
        args += ['--stats-dir', os.path.join(output_dir, "Stats" + task.suffix)]
    if task.lanes:
        args += ['--tiles', "s_[" + "".join(str(l) for l in task.lanes) + "]"]
    args += get_thread_args(n_threads)
    if other_options:
        args += re.split(" +", other_options)

    log_path = task.logfile("bcl2fastq2")
    jobname = "bcl2fastq2"
    if task.process:
        jobname = task.process.id + "." + jobname

    print "Calling bcl2fastq with:", " ".join(args)

    rcode = remote.run_command(
            args, jobname, time="1-0", logfile=log_path,
            cpus=n_threads, mem="15G", comment=comment
            )

    # LIMS only:
    # - Upload log
    # - Get the bcl2fastq version
    if task.process:
        try:
            utilities.upload_file(task.process, nsc.BCL2FASTQ_LOG, log_path)
            log = open(log_path)
            log_iter = iter(log)
            for l,i in zip(log_iter,range(3)):
                if l.startswith("bcl2fastq v"):
                    task.process.udf[nsc.BCL2FASTQ_VERSION_UDF] = l.split(" ")[1].strip("\n")
                    # Will put() when calling success_finish() or fail()
        except IOError:
            pass

    return rcode == 0



if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)


