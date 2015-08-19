# Simple script to run fastqc. Takes the input names from the sample 
# sheet.

import os
import re
import shutil
from genologics.lims import *
from common import utilities, samples, nsc, taskmgr

TASK_NAME = "Run FastQC"
TASK_DESCRIPTION = """Run FastQC on the demultiplexed files."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'threads']

def main(task):

    task.running()
    os.umask(007)

    run_id = task.run_id
    bc_dir = task.bc_dir

    projects = task.projects


    # Empty fastq files (no sequences) will not be created by the new 
    # bcl2fastq, instead they will not exist, so we check that.
    # First get a list of FastqFile objects
    fastq_files = [
            f
            for p in projects for s in p.samples for f in s.files
            if os.path.exists(os.path.join(bc_dir, f))
            ]
    # then get the paths
    fastq_paths = [
            os.path.join(bc_dir, f.path) for f in fastq_files
            ]


    output_dir = os.path.join(bc_dir, "QualityControl")
    try:
        os.mkdir(output_dir)
    except OSError:
        pass

    threads = task.threads

    fastqc_args = ["--extract", "--threads=" + str(threads)]
    fastqc_args += ["--outdir=" + output_dir]
    fastqc_args += fastq_files

    log_path = task.logfile("fastqc")
    if task.process:
        jobname = task.process.id + ".fastqc"
    else:
        jobname = "fastqc"
    rcode = slurm.srun_command(
            [nsc.FASTQC] + fastqc_args, jobname, time="1-0", logfile=log_path,
            cpus_per_task=n_threads, mem=str(1024+256*threads)+"M"
            )

    if rcode == 0:
        move_fastqc_results(output_dir, projects)
        task.success_finish()
    else:
        task.fail("fastqc failure")

    return rcode


def fastqc_dir(basecalls_dir, fastq_path):
    """Get path to directory written by fastqc"""
    return re.sub(r".fastq.gz$", "_fastqc", os.path.basename(fastq_path))


def move_fastqc_results(qc_dir, basecalls_dir, projects):
    """Organises the fastqc results into a more manageable structure. Deletes zip files.
    Gets the desired name of the fastqc dir from the "samples" module."""

    for project in projects:
        # Create the project dir
        try:
            os.mkdir(os.path.join(qc_dir, project.name))
        except OSError:
            pass

        for sample in project.samples:
            try:
                os.mkdir(os.path.join(qc_dir, project.name, sample.name))
            except OSError:
                pass

            for f in sample.files:
                # Have to check again if the fastq file exists, to determine if 
                # fastqc was run on it
                if os.path.exists(basecalls_dir, f.path):
                    original_fqc_dir = fastqc_dir(basecalls_dir, f.path)
                    os.remove(original_fqc_dir + ".zip")
                    fqc_dir = os.path.join(qc_dir, samples.get_fastqc_dir(project, sample, f))
                    if os.path.exists(fqc_dir):
                        shutil.rmtree(fqc_dir)
                    os.rename(original_fqc_dir, fqc_dir)


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

