# Generate Demultiplex_Stats.htm for each project

import os
import demultiplex_stats
from xml.etree import ElementTree

from common import nsc, stats, samples, utilities, taskmgr

template_dir = os.path.dirname(os.path.realpath(__file__)) + "/template"

TASK_NAME = "70. Demultiplex_stats"
TASK_DESCRIPTION = """Generates Demultiplex_Stats.htm for each project."""

TASK_ARGS = ['work_dir', 'sample_sheet']


def main(task):
    task.running()
    run_id = task.run_id
    instrument = utilities.get_instrument_by_runid(run_id)
    fcid = utilities.get_fcid_by_runid(task.run_id)
    work_dir = task.work_dir
    basecalls_dir = task.bc_dir
    projects = task.projects
    qc_dir = os.path.join(basecalls_dir, "QualityControl" + task.suffix)

    if task.process:
        bcl2fastq_version = utilities.get_udf(task.process, nsc.BCL2FASTQ_VERSION_UDF, None)
    else:
        bcl2fastq_version = utilities.get_bcl2fastq2_version(work_dir)
        if not bcl2fastq_version:
            task.warn("bcl2fastq version cannot be detected, use the --bcl2fastq-version option to specify!")

    real_projects = []
    for project in projects:
        if project.is_undetermined:
            undetermined_project = project
        else:
            real_projects.append(project)

    for project in real_projects:
        demultiplex_stats_content = demultiplex_stats.demultiplex_stats(
                project, undetermined_project, work_dir, basecalls_dir, instrument,
                task.no_lane_splitting, fcid, bcl2fastq_version, task.suffix
                )

        with open(os.path.join(qc_dir, project.name, "Demultiplex_Stats.htm"), 'w') as f:
            f.write(demultiplex_stats_content)

    task.success_finish()



if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)


