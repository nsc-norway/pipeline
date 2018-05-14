# Simple script to run fastqc. Takes the input names from the sample 
# sheet.

import os
import re
import shutil
import time
from common import samples, nsc, taskmgr, samples, remote, utilities

TASK_NAME = "50. QC analysis"
TASK_DESCRIPTION = """Run QC tools on the demultiplexed files."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'threads', 'lanes']

def main(task):
    task.running()

    run_id = task.run_id
    bc_dir = task.bc_dir

    projects = task.projects
    samples.flag_empty_files(projects, task.work_dir)

    output_dir = os.path.join(bc_dir, "QualityControl" + task.suffix)
    try:
        os.mkdir(output_dir)
    except OSError:
        pass

    fqc_log_path = task.logfile("fastqc")
    dup_log_path = task.logfile("fastdup")

    if os.path.exists(fqc_log_path):
        with open(fqc_log_path, 'w') as f:
            f.truncate()

    # This loop has two purposes: 
    # - Generate commands for fastdup and fastqc
    #   (Dupe commands are the same for scheduler mode and normal mode.)
    # - Create the directory structure for results for FastQC and fastdup
    dup_commands = []
    fqc_commands = []
    fastqc_zipfiles = []
    file_sizes = []
    for project in projects:
        if project.is_undetermined:
            project_dir = os.path.join(output_dir, "Undetermined")
        else:
            project_dir = os.path.join(output_dir, project.name)
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
                    file_fastqc_dir = samples.get_fastqc_dir(project, sample, f)
                    fqc_basedir = os.path.join( output_dir, os.path.dirname(file_fastqc_dir))
                    fq_path = os.path.join(bc_dir, f.path)
                    fqc_commands.append([nsc.FASTQC, "--extract",
                            "--outdir=" + fqc_basedir,
                            fq_path
                            ])
                    file_sizes.append(os.path.getsize(fq_path))
                    fastqc_zipfiles.append(os.path.join(output_dir, file_fastqc_dir + ".zip"))
                    if f.i_read == 1:
                        output_path = os.path.join(
                                output_dir,
                                samples.get_fastdup_path(project, sample, f),
                                )
                        dup_commands.append(
                                nsc.FASTDUP_ARGLIST + [
                                    os.path.join(bc_dir, f.path),
                                    output_path
                                    ]
                                )
    

    fqc = remote.ArrayJob(fqc_commands, "fastqc", "1-0",
            fqc_log_path.replace(".txt", ".%a.txt"))
    fqc.mem_per_task = 1900
    fqc.cpus_per_task = 1
    fqc.bandwidth_per_task = "20M" # Odin: "50M"
    fqc.comment = run_id
    jobs = []

    # FastQC crashes is too many jobs complete on the same node at the same time
    # For small jobs (< about 500k reads), this may be a problem, and we reduce the max
    # simultaneous jobs to be safe. Use median size 50MB as cut-off; parameters may need
    # tuning.
    try:
        median_size = list(sorted(file_sizes))[len(file_sizes)/2]
        if median_size < 50 * 1024**2:
            fqc.max_simultaneous = 10
    except IndexError:
        pass

    if task.instrument in ["hiseqx", "hiseq4k"] and nsc.FASTDUP != None:
        dup = remote.ArrayJob(dup_commands, "fastdup", "6:00:00", 
                dup_log_path.replace(".txt", ".%a.txt"))
        dup.mem_per_task = 500
        dup.cpus_per_task = 1
        dup.bandwidth_per_task = "30M" # Odin: 160M
        dup.comment = run_id
        jobs = [fqc, dup]
    else:
        jobs = [fqc]
        dup = None

    remote.ArrayJob.start_jobs(jobs, max_local_threads=task.threads)

    task.array_job_status(jobs)
    delay = 1
    while not all(job.is_finished for job in jobs):
        time.sleep(delay)
        delay = 30
        remote.ArrayJob.update_status(jobs)
        task.array_job_status(jobs)
    
    fail = ""
    detail = ""
    if fqc.summary.keys() != ["COMPLETED"]:
        fail += "fastqc failure "
        detail = str(fqc.summary)
    if dup and dup.summary.keys() != ["COMPLETED"]:
        fail += "fastdup failure "
        detail = str(dup.summary)
    if fail:
        task.fail(fail, detail)
	
    for zipfile in fastqc_zipfiles:
        try:
            os.remove(zipfile)
        except OSError:
            pass

    task.success_finish() # Calls exit(0)


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

