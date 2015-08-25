# Simple script to run fastqc. Takes the input names from the sample 
# sheet.

import os
import re
import shutil
from common import samples, nsc, taskmgr, samples, slurm, utilities

TASK_NAME = "FastQC"
TASK_DESCRIPTION = """Run FastQC on the demultiplexed files."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'threads']

def main(task):
    task.running()

    run_id = task.run_id
    bc_dir = task.bc_dir

    projects = task.projects
    samples.flag_empty_files(projects, task.work_dir)

    # First get a list of FastqFile objects
    # Empty fastq files (no sequences) will not be created by the new 
    # bcl2fastq so skip those
    fastq_files = [
            f
            for p in projects for s in p.samples for f in s.files
            if not f.empty
            ]
    # then get the paths
    # Put large files first in the list, so that the fastqc process won't be
    # stuck processing one or two large files long after all others have finished
    fastq_paths = sorted(
            (os.path.join(bc_dir, f.path) for f in fastq_files),
            key=os.path.getsize,
            reverse=True
            )

    output_dir = os.path.join(bc_dir, "QualityControl")
    try:
        os.mkdir(output_dir)
    except OSError:
        pass

    threads = task.threads

    fastqc_args = ["--extract", "--threads=" + str(threads)]
    fastqc_args += ["--outdir=" + output_dir]

    if task.process:
        jobname = task.process.id + ".fastqc"
    else:
        jobname = "fastqc"

    log_path = task.logfile("fastqc")
    try:
        with open(log_path, 'w') as f:
            f.truncate()
    except IOError:
        pass

    # Process the files in groups of 500 files to prevent the 
    # "argument list too long" error. The groups are processed 
    # sequentially. The +499 is a "trick" to round up with int
    # division.
    n_groups = (len(fastq_paths) + 499) // 500
    for i_group in xrange(n_groups):
        # process e.g. #1, #3, #5, ... then #2, #4, #6... for 2k files
        proc_paths = fastq_paths[i_group::n_groups]
        grp_fastqc_args = fastqc_args + proc_paths
        print "Fastqc-" + str(i_group), ": Processing", len(proc_paths), "of", len(fastq_paths), "files..."
        rcode = slurm.srun_command(
                [nsc.FASTQC] + grp_fastqc_args, jobname, time="1-0", 
                logfile=log_path, cpus_per_task=threads,
                mem=str(1024+256*threads)+"M",
                srun_user_args=['--open-mode=append'],
                change_user=task.process != None
                )

        if rcode != 0:
            # The following function will call exit(1)
            task.fail("fastqc failure", "Group " + str(i_group))

    
    if task.process:
        utilities.upload_file(task.process, nsc.FASTQC_LOG, log_path)

    move_fastqc_results(output_dir, projects)
    task.success_finish() # Calls exit(0)



def fastqc_dir(fastq_path):
    """Get name of directory written by fastqc"""
    return re.sub(r".fastq.gz$", "_fastqc", os.path.basename(fastq_path))


def move_fastqc_results(qc_dir, projects):
    """Organises the fastqc results into a more manageable structure. Deletes zip files.
    Gets the desired name of the fastqc dir from the "samples" module."""

    for project in projects:
        # Create the project dir
        if project.name:
            project_dir = os.path.join(qc_dir, project.name)
        else:
            project_dir = os.path.join(qc_dir, "Undetermined")

        if not os.path.exists(project_dir):
            os.mkdir(project_dir)

        for sample in project.samples:
            for f in sample.files:
                # Have to check again if the fastq file exists, to determine if 
                # fastqc was run on it
                if not f.empty:
                    original_fqc_dir = os.path.join(qc_dir, fastqc_dir(f.path))
                    try:
                        os.remove(original_fqc_dir + ".zip")
                    except OSError:
                        pass
                    fqc_dir = os.path.join(qc_dir, samples.get_fastqc_dir(project, sample, f))
                    if os.path.exists(fqc_dir):
                        shutil.rmtree(fqc_dir)

                    os.rename(original_fqc_dir, fqc_dir)
                    os.rename(original_fqc_dir + ".html", fqc_dir + ".html")


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

