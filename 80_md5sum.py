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
    samples.add_index_read_files(projects, task.work_dir)
    for project in projects:
        if not project.is_undetermined:
            pathses = [paths_for_project(run_id, project)]
            rcode = 0
            task.info(project.name)
            while pathses: 
                try:
                    for i, paths in enumerate(pathses):
                        partial_stdout = os.path.join(bc_dir, project.proj_dir, "md5sum_{}.txt".format(i))
                        if paths:
                            rcode = rcode | remote.run_command(
                                    nsc.MD5 + paths, task, "md5deep",
                                    time="08:00:00", cpus=n_threads, mem="2048M",
                                    cwd=os.path.join(bc_dir, project.proj_dir),
                                    stdout=partial_stdout, comment=run_id
                                    )
                        else:
                            with open(partial_stdout, 'w'):
                                pass
                    num_jobs = len(pathses) # The command suceeded
                    pathses = None
                except OSError as e:
                    if e.args[0] == 7: #Argument list too long!
                        pathss2 = []
                        for paths in pathses:
                            pathss2.append(paths[:len(paths)//2])
                            pathss2.append(paths[len(paths)//2:])
                        pathses = pathss2
                    else:
                        raise
            if rcode == 0:
                # Combine all md5sum jobs
                stdout = os.path.join(bc_dir, project.proj_dir, "md5sum.txt")
                with open(stdout, 'w') as outfile:
                    for i in range(num_jobs):
                        partial_stdout = os.path.join(bc_dir, project.proj_dir, "md5sum_{}.txt".format(i))
                        with open(partial_stdout) as infile:
                            outfile.write(infile.read())
                        os.unlink(partial_stdout)
            else:
                task.fail(
                        "md5deep failed for project " + project.name,
                        "\n".join(paths)
                        )

    task.success_finish()



if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)
