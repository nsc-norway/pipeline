#!/bin/env python

# Generate checksum for fastq files and pdf files


import os
from common import taskmgr, samples, slurm

TASK_NAME = "Checksums"
TASK_DESCRIPTION = """Compute md5 checksums for fastq files and pdfs."""

TASK_ARGS = ['work_dir', 'sample_sheet', 'threads']


def paths_for_project(run_id, project):
    paths = []
    for sample in project.samples:
        for f in sample.files:
            if not f.empty:

                path = f.filename
                pdfpath = samples.qc_pdf_name(run_id, f)
                if sample.sample_dir:
                    path = os.path.join(sample.sample_dir, path)
                    pdfpath = os.path.join(sample.sample_dir, pdfpath)

                paths.append(path)
                paths.append(pdfpath)

    return paths



def main(task):
    task.running()
    bc_dir = task.bc_dir
    run_id = task.run_id
    n_threads = task.threads
    projects = task.projects
    samples.flag_empty_files(projects, work_dir)
    for project in projects:
        if not project.is_undetermined:

            paths = paths_for_project(run_id, project)

            rcode = slurm.srun_command(
                    [nsc.MD5DEEP] + paths, jobname, time="02:00:00",
                    cpus_per_task=n_threads, mem="1024M",
                    cwd=os.path.join(bc_dir, project.project_dir)
                    )

            if rcode != 0:
                task.fail(
                        "md5deep failed for project " + project.name,
                        "\n".join(paths)
                        )

    task.success_finish()



if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)
