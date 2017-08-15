#!/bin/env python

# Generate checksum for fastq files and pdf files


import os
from common import nsc, taskmgr, samples, remote

TASK_NAME = "80. Checksums"
TASK_DESCRIPTION = """Compute md5 checksums for fastq files and pdfs."""

TASK_ARGS = ['work_dir', 'sample_sheet', 'threads', 'lanes']


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
    n_threads = min(task.threads, 5)
    projects = task.projects
    samples.flag_empty_files(projects, task.work_dir)
    for project in projects:
        if not project.is_undetermined:

            paths = paths_for_project(run_id, project)
            task.info(project.name)
            if not paths:
                continue # No files to check
            stdout = os.path.join(bc_dir, project.proj_dir, "md5sum.txt")
            rcode = remote.run_command(
                    [nsc.MD5DEEP, '-rl', '-j' + str(n_threads)] + paths, task, "md5deep",
                    time="08:00:00", cpus=n_threads, mem="2048M", bandwidth=str(n_threads*1.2) + "G",
                    storage_job=True, cwd=os.path.join(bc_dir, project.proj_dir),
                    stdout = stdout
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
