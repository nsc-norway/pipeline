import sys
import os
import operator
from collections import defaultdict

from common import nsc, stats, utilities, lane_info, samples, taskmgr

TASK_NAME = "60. Emails"
TASK_DESCRIPTION = """Produce delivery reports for emails."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'lanes']


# Generate reports for emails based on demultiplexing stats


def main(task):
    task.running()
    
    run_id = task.run_id
    work_dir = task.work_dir
    instrument = utilities.get_instrument_by_runid(run_id)
    
    try:
        lane_stats = lane_info.get_from_interop(task.work_dir, task.no_lane_splitting)
    except: # lane_info.NotSupportedException:
        expand_lanes = instrument == "nextseq" and not task.no_lane_splitting

        if task.process: # lims mode
            lane_stats = lane_info.get_from_lims(task.process, instrument, expand_lanes)
        else:
            lane_stats = lane_info.get_from_files(work_dir, instrument, expand_lanes)

    projects = task.projects

    run_stats = stats.get_stats(
            instrument,
            work_dir,
            aggregate_lanes = task.no_lane_splitting,
            aggregate_reads = False,
            suffix=task.suffix
            )
    samples.add_stats(projects, run_stats)
    if task.instrument in ["hiseq4k", "hiseqx"]:
        qc_dir = os.path.join(task.bc_dir, "QualityControl" + task.suffix)
        stats.add_duplication_results(qc_dir, projects)
    samples.flag_empty_files(projects, work_dir)

    qc_dir = os.path.join(work_dir, "Data", "Intensities", "BaseCalls", "QualityControl" + task.suffix)
    make_reports(instrument, qc_dir, run_id, projects, lane_stats, task.lims, task.process)

    task.success_finish()


def make_reports(instrument_type, qc_dir, run_id, projects, lane_stats, lims, process):
    if not os.path.exists(qc_dir):
        os.mkdir(qc_dir)
    delivery_dir = os.path.join(qc_dir, "Delivery")
    if not os.path.exists(delivery_dir):
        os.mkdir(delivery_dir)

    for project in projects:
        if not project.is_undetermined:
            fname = delivery_dir + "/Email_for_" + project.name + ".xls"
            write_sample_info_table(fname, run_id, project)

    fname = delivery_dir + "/Summary_email_for_NSC_" + run_id + ".xls"
    patterned = instrument_type in ["hiseqx", "hiseq4k"]
    write_summary_email(fname, run_id, projects, instrument_type.startswith('hiseq'), lane_stats, patterned)

    if process:
        inputs = process.all_inputs(unique=True, resolve=True)
        samples = lims.get_batch((sample for i in inputs for sample in i.samples))
        lims_projects = dict(
                (utilities.get_sample_sheet_proj_name(sample.project.name), sample.project)
                for sample in samples
                if sample.project
                )
        for project in task.projects:
            lims_project = lims_projects.get(project.name)
            if lims_project:
                fname = delivery_dir + "/LIMS_for_" + project.name + ".txt"
                write_lims_info(fname, run_id, project, lims, lims_project)


def write_sample_info_table(output_path, runid, project):
    with open(output_path, 'w') as out:
        out.write('--------------------------------		\r\n')
        out.write('Email for ' + project.name + "\r\n")
        out.write('--------------------------------		\r\n\r\n')
        nsamples = len(project.samples)
        out.write('Sequence ready for download - sequencing run ' + runid + ' - Project_' + project.name + ' (' + str(nsamples) + ' samples)\r\n\r\n')

        if project.name.startswith("Diag-"):
            files = sorted(
                    ((s,fi) for s in project.samples for fi in s.files if fi.i_read == 1),
                    key=lambda (s,f): (f.lane, s.sample_index, f.i_read)
                    )
            for i, (s,f) in enumerate(files, 1):
                out.write("Sample\t" + str(i) + "\t")
                if f.empty:
                    out.write("0\t")
                else:
                    out.write(utilities.display_int(f.stats['# Reads PF']) + "\t")
                out.write("fragments\r\n")
        else:
            files = sorted(
                    ((s,fi) for s in project.samples for fi in s.files),
                    key=lambda (s,f): (f.lane, s.sample_index, f.i_read)
                    )
            for s,f in files:
                out.write(os.path.basename(f.path) + "\t")
                if f.empty:
                    out.write("0\t")
                else:
                    out.write(utilities.display_int(f.stats['# Reads PF']) + "\t")
                out.write("fragments\r\n")


def write_lims_info(output_path, runid, project, lims, lims_project):
    with open(output_path, 'w') as out:
        out.write('--------------------------------		\n')
        out.write('LIMS info for ' + project.name + "\n")
        out.write('--------------------------------		\n\n')

        for key in ["Contact person", "Contact email", "Number of lanes"]:
            val = lims_project.udf.get(key, "")
            if isinstance(val, type(u"")):
                out.write(key + ":\t" + val.encode('utf-8') + "\n")
            else:
                out.write(key + ":\t" + str(val) + "\n")

        out.write("Completed lanes:\t")
        completed_runs = lims.get_processes(
                type=(t[1] for t in nsc.SEQ_PROCESSES),
                projectname=lims_project.name
                )
        completed_lanes_all = sum(
                (run_process.all_inputs(unique=True)
                for run_process in completed_runs),
                []
                )
        completed_lanes = set(lane.stateless for lane in completed_lanes_all)
        lims.get_batch(completed_lanes)
        lims.get_batch(lane.samples[0] for lane in completed_lanes)
        state_count = defaultdict(int)
        for lane in completed_lanes:
            if lane.samples[0].project == lims_project:
                state_count[lane.qc_flag]+=1
        out.write(", ".join(state + ": " + str(count) for state, count in state_count.items()) + "\n")
        out.write("Lanes in this run:\t" + str(len(set(f.lane for sample in project.samples for f in sample.files))) + "\n")


def write_summary_email(output_path, runid, projects, print_lane_number, lane_stats, patterned):
    with open(output_path, 'w') as out:
        summary_email_head = """\r
--------------------------------							\r
Summary email to NSC members							\r
--------------------------------							\r
							\r
Summary for run {runId}							\r
							\r
PF = pass illumina filter							\r
PF cluster no = number of PF cluster in the lane							\r
Undetermined ratio = how much proportion of fragments can not be assigned to a sample in the indexed lane							\r
Quality = summary of the overall quality							\r
\r
""".format(runId = runid)
        if print_lane_number:
            if patterned:
                summary_email_head += """\r
Lane\tProject\tPF cluster no\tPF ratio\tSeqDuplicates\tUndetermined ratio\tAlignedPhiX\t>=Q30\tQuality\r
"""
            else:
                summary_email_head += """\r
Lane\tProject\tPF cluster no\tPF ratio\tRaw cluster density(/mm2)\tPF cluster density(/mm2)\tUndetermined ratio\tAlignedPhiX\t>=Q30\tQuality\r
"""
        else:
            summary_email_head += """\r
Project\tPF cluster no\tPF ratio\tRaw cluster density(/mm2)\tPF cluster density(/mm2)\tUndetermined ratio\tAlignedPhiX\t>=Q30\tQuality\r
"""

        out.write(summary_email_head)
        # assumes 1 project per lane, and undetermined
        # Dict: lane ID => (lane object, project object)
        lane_proj = dict((f.lane, proj) for proj in projects
                for s in proj.samples for f in s.files if not proj.is_undetermined)
        # lane ID => file object (will be the last one)
        lane_undetermined = dict(
                (f.lane, f)
                for proj in projects
                for s in proj.samples
                for f in s.files
                if proj.is_undetermined)

        for l in sorted(lane_proj.keys()):
            proj = lane_proj[l]
            undetermined_file = lane_undetermined.get(l)

            if print_lane_number:
                out.write(str(l) + "\t")
            out.write(proj.name + "\t")
            cluster_no = sum(f.stats['# Reads PF']
                    for proj in projects
                    for s in proj.samples
                    for f in s.files
                    if f.lane == l and f.i_read == 1 and not f.empty)
            out.write(utilities.display_int(cluster_no) + '\t')
            lane = lane_stats[l]
            out.write("%4.2f" % (lane.pf_ratio if lane.pf_ratio is not None else 0.0) + "\t")
            if not patterned:
                out.write(utilities.display_int(lane.cluster_den_raw) + '\t')
                out.write(utilities.display_int(lane.cluster_den_pf) + '\t')

            if patterned:
                dupsum = sum(f.stats['fastdup reads with duplicate']
                        for proj in projects
                        for s in proj.samples
                        for f in s.files
                        if f.lane == l and f.i_read == 1 and not f.empty)
                try:
                    duppct = dupsum * 100.0 / cluster_no
                    out.write("%4.2f%%\t" % duppct)
                except ZeroDivisionError:
                    out.write("-\t")

            if undetermined_file and not undetermined_file.empty:
                try:
                    out.write("%4.2f" % (undetermined_file.stats['% of PF Clusters Per Lane'],) + "%\t")
                except KeyError:
                    out.write("%4s" % ('?') + "%\t")
            else:
                out.write("-\t")

            if lane.phix is None:
                out.write("-\t")
            else:
                out.write("%4.2f%%\t" % (lane.phix * 100.0))

            q30sum = sum(f.stats['% Bases >=Q30']*f.stats['# Reads PF']
                    for proj in projects
                    for s in proj.samples
                    for f in s.files
                    if f.lane == l and not f.empty)
            norm = sum(f.stats['# Reads PF']
                    for proj in projects
                    for s in proj.samples
                    for f in s.files
                    if f.lane == l and not f.empty)
            q30pct = q30sum  / max(norm, 1)
            out.write("%4.2f%%\t" % q30pct)

            out.write("ok\r\n")


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

