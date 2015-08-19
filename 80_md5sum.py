#!/bin/env python

# Generate checksum for fastq files and pdf files


import os
from common import taskmgr, samples, slurm

TASK_NAME = "Checksums"
TASK_DESCRIPTION = """Compute md5 checksums for fastq files and pdfs."""

TASK_ARGS = ['work_dir', 'sample_sheet', 'threads']


def paths_for_project(project):

    # Prefix of the sample names
    prefix = project.proj_dir.rstrip("/") + "/"

    paths = []
    for f in (f for s in project.samples for f in s.files):
        if not f.path.startswith(prefix):
            task.fail("Unexpected filename!", f.path)
            # exits

        # Files with no reads will not be written, so we skip them
        if os.path.exists(os.path.join(bc_dir, f.path)):

            relpath = f.path[len(prefix):]
            paths.append(relpath)

            pdfname = samples.qc_pdf_name(run_id, f)
            relpath_dir = os.path.dirname(relpath)

            paths.append(os.path.join(relpath_dir, pdfname))
    return paths



def main(task):
    task.running()
    bc_dir = task.bc_dir
    run_id = task.run_id
    n_threads = task.threads

    for project in task.projects:
        if not project.is_undetermined:

            paths = paths_for_project(project)

            rcode = slurm.srun_command(
                    [nsc.MD5DEEP] + paths, jobname, time="1-0",
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
