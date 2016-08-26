# Simple script to run fastqc. Takes the input names from the sample 
# sheet.

import os
import re
import shutil
import time
from common import samples, nsc, taskmgr, samples, remote, utilities

TASK_NAME = "60. QC analysis"
TASK_DESCRIPTION = """Run QC tools on the demultiplexed files."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'threads', 'lanes']

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

    output_dir = os.path.join(bc_dir, "QualityControl" + task.suffix)
    try:
        os.mkdir(output_dir)
    except OSError:
        pass

    threads = task.threads

    fastqc_args = ["--extract", "--outdir=" + output_dir]

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


    if remote.is_scheduler_available():
        commands = [[nsc.FASTQC] + fastqc_args + [path] for path in fastq_paths]
        jobs = []
        fqc = remote.ArrayJob(commands, jobname, "1-0", log_path.replace(".txt", ".%a.txt"))
        fqc.cpus_per_task = 1
        fqc.mem_per_task = 1
        fqc.max_simultaneous = 64 # Limit due to I/O bottlenecks
        # This is highly dependent on the computing environment. Should be configurable, or
        # maybe the admin could limit the number of jobs in slurm.
        fqc.start()

        jobs = [fqc]

        task.array_job_status(jobs)
        while not all(job.is_finished for job in jobs):
            time.sleep(30)
            for job in jobs:
                job.check_status()
            task.array_job_status(jobs)
        
        if sum((job.summary.keys() for job in jobs), {}) != ["COMPLETED"]:
            task.fail("fastqc failure", str(aj.summary))
	
    else:
        # Process the files in groups of 500 files to prevent the 
        # "argument list too long" error. The groups are processed 
        # sequentially. 
        n_groups = (len(fastq_paths) + 499) // 500
        for i_group in xrange(n_groups):
            # process interleaved e.g. #1, #3, #5, ... then #2, #4, #6...
            # to preserve order
            proc_paths = fastq_paths[i_group::n_groups]
            task.info("Fastqc-{0}: Processing {1} of {2} files...".format(i_group, len(proc_paths), len(fastq_paths)))

            threads_to_request=min(len(proc_paths), threads)
            grp_fastqc_args = fastqc_args + ["--threads=" + str(threads)] + proc_paths
            rcode = remote.run_command(
                    [nsc.FASTQC] + grp_fastqc_args, jobname, time="1-0", 
                    logfile=log_path, cpus=threads_to_request,
                    mem=str(1024+256*threads)+"M",
                    srun_user_args=['--open-mode=append']
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
        # Create the project dir (and sample dir below)
        # while most of the code only depends on samples.get_fastqc_dir
        # for locating the fastqc files, this function also hast to 
        # create the directories in between 
        if project.is_undetermined:
            project_dir = os.path.join(qc_dir, "Undetermined")
        else:
            project_dir = os.path.join(qc_dir, project.name)

        if not os.path.exists(project_dir):
            os.mkdir(project_dir)

        for sample in project.samples:
            if project.is_undetermined:
                sample_dir = os.path.join(project_dir, "Sample_Undetermined")
            else:
                sample_dir = os.path.join(project_dir, "Sample_" + sample.name)

            if not os.path.exists(sample_dir):
                os.mkdir(sample_dir)

            for f in sample.files:
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

