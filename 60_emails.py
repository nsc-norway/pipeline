import sys
import os
import re
import operator
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader
try:
    from jinja2 import select_autoescape
except ImportError:
    select_autoescape = None

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


    fname_html = delivery_dir + "/Emails_for_" + run_id + ".html"
    template_dir = os.path.join(os.path.dirname(__file__), "template")
    if select_autoescape is None:
        jinja_env = Environment(loader=FileSystemLoader(template_dir))
    else:
        jinja_env = Environment(loader=FileSystemLoader(template_dir), autoescape=select_autoescape(['html','xml']))
    write_html_file(jinja_env, process, fname_html, run_id, projects, instrument_type.startswith('hiseq'), lane_stats, patterned)



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


def get_lane_summary_data(projects, print_lane_number, lane_stats, patterned):
    if print_lane_number:
        if patterned:
            header = ["Lane", "Project", "PF cluster no", "PF ratio", "SeqDuplicates", "Undetermined", "AlignedPhiX", ">=Q30", "Quality"]
        else:
            header = ["Lane", "Project", "PF cluster no", "PF ratio", "Raw cluster density(/mm2)", "PF cluster density(/mm2)", "Undetermined",
                    "AlignedPhiX", ">=Q30", "Quality"]
    else:
        header = ["Project", "PF cluster no", "PF ratio", "Raw cluster density(/mm2)", "PF cluster density(/mm2)", "Undetermined", "AlignedPhiX",
                ">=Q30", "Quality"]

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

    data = []

    for l in sorted(lane_proj.keys()):
        row = []
        proj = lane_proj[l]
        undetermined_file = lane_undetermined.get(l)

        if print_lane_number:
            row.append((l, "center"))
        row.append((proj.name, "text"))

        cluster_no = sum(f.stats['# Reads PF']
                for proj in projects
                for s in proj.samples
                for f in s.files
                if f.lane == l and f.i_read == 1 and not f.empty)
        row.append((utilities.display_int(cluster_no), "number"))
        lane = lane_stats[l]
        row.append(("%4.2f" % (lane.pf_ratio if lane.pf_ratio is not None else 0.0), "number"))
        if not patterned:
            row.append((utilities.display_int(lane.cluster_den_raw), "number"))
            row.append((utilities.display_int(lane.cluster_den_pf), "number"))

        if patterned:
            dupsum = sum(f.stats['fastdup reads with duplicate']
                    for proj in projects
                    for s in proj.samples
                    for f in s.files
                    if f.lane == l and f.i_read == 1 and not f.empty)
            try:
                duppct = dupsum * 100.0 / cluster_no
                row.append(("%4.2f %%" % duppct, "number"))
            except ZeroDivisionError:
                row.append("-")

        if undetermined_file and not undetermined_file.empty:
            try:
                row.append(("%4.2f %%" % (undetermined_file.stats['% of PF Clusters Per Lane'],), "number"))
            except KeyError:
                row.append(("? %", "number"))
        else:
            row.append(("-", "center"))

        if lane.phix is None:
            row.append(("-", "center"))
        else:
            row.append(("%4.2f%%" % (lane.phix * 100.0), "number"))

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
        row.append(("%4.2f%%" % q30pct, "number"))
        row.append(("ok", "text"))
        data.append(row)
    return header, data


class ProjectData(object):
    def __init__(self, project, lims_project, seq_process):
        self.nsamples = len(project.samples)
        self.name = project.name
        self.file_fragments_table = []
        self.dir = project.proj_dir
        if project.name.startswith("Diag-"):
            files = sorted(
                    ((s,fi) for s in project.samples for fi in s.files if fi.i_read == 1),
                    key=lambda (s,f): (f.lane, s.sample_index, f.i_read)
                    )
            for i, (s,f) in enumerate(files, 1):
                sample = "Sample {0}".format(i)
                if f.empty:
                    self.file_fragments_table.append((sample, "0"))
                else:
                    self.file_fragments_table.append((sample, utilities.display_int(f.stats['# Reads PF'])))
        else:
            files = sorted(
                    ((s,fi) for s in project.samples for fi in s.files),
                    key=lambda (s,f): (f.lane, s.sample_index, f.i_read)
                    )
            for s,f in files:
                filename = os.path.basename(f.path)
                if f.empty:
                    self.file_fragments_table.append((filename, "0"))
                else:
                    self.file_fragments_table.append((filename, utilities.display_int(f.stats['# Reads PF'])))
        if lims_project:
            self.lims = LimsInfo(lims_project, seq_process)


class RunParameters(object):
    def __init__(self, run_id, process):
        """Get run parameters (LIMS based)"""
        self.instrument_type = utilities.get_instrument_by_runid(run_id)
        self.instrument_name = process.udf.get('Instrument Name')
        id_part = re.match(r"\d\d\d\d\d\d_([^_]+)_", run_id)
        if id_part:
            self.instrument_id = id_part.group(1)
        else:
            self.instrument_id = None
        self.cycles = [("R1", process.udf.get('Read 1 Cycles'))]
        cys = process.udf.get('Index 1 Read Cycles')
        if cys:
            self.cycles.append(("I1", cys))
        cys = process.udf.get('Index 2 Read Cycles')
        if cys:
            self.cycles.append(("I2", cys))
        cys = process.udf.get('Read 2 Cycles')
        if cys:
            self.cycles.append(("R2", cys))
        pars = ["Chemistry", "Run Mode", "Chemistry Version"]
        self.run_mode_field = None
        for par in pars:
            p = process.udf.get(par)
            if p:
                self.run_mode_field = par
                self.run_mode_value = p


class LimsInfo(object):
    def __init__(self, lims_project, seq_process):
        self.contact_person = lims_project.udf.get('Contact person')
        self.contact_email = lims_project.udf.get('Contact email')
        self.total_number_of_lanes = lims_project.udf.get('Number of lanes')
        completed_runs = lims_project.lims.get_processes(
                type=(t[1] for t in nsc.SEQ_PROCESSES),
                projectname=lims_project.name
                )
        completed_lanes_all = sum(
                (run_process.all_inputs(unique=True)
                for run_process in completed_runs),
                []
                )
        completed_lanes = set(lane.stateless for lane in completed_lanes_all)
        lims_project.lims.get_batch(completed_lanes)
        lims_project.lims.get_batch(lane.samples[0] for lane in completed_lanes)
        state_count = defaultdict(int)
        this_run_lanes = set(seq_process.all_inputs())
        for lane in completed_lanes:
            if lane in this_run_lanes:
                state = "THIS_RUN"
            else:
                state = lane.qc_flag
            if lane.samples[0].project == lims_project:
                state_count[state]+=1
        self.sequencing_status = ", ".join(str(k) + ": " + str(v) for k, v in state_count.items())


def write_html_file(jinja_env, process, output_path, runid, projects, print_lane_number, lane_stats, patterned):
    """Stats summary file for emails, etc."""

    project_datas = []
    if process:
        seq_process = utilities.get_sequencing_process(process)
        inputs = process.all_inputs(unique=True, resolve=True)
        samples = process.lims.get_batch((sample for i in inputs for sample in i.samples))
        lims_projects = dict(
                (utilities.get_sample_sheet_proj_name(sample.project.name), sample.project)
                for sample in samples
                if sample.project
                )
    else:
        lims_projects = {}
        seq_process = None
    for project in projects:
        if not project.is_undetermined:
            lims_project = lims_projects.get(project.name)
            project_datas.append(ProjectData(project, lims_project, seq_process))

    lane_header, lane_data = get_lane_summary_data(projects, print_lane_number, lane_stats, patterned)
    if process:
        run_parameters = RunParameters(runid, seq_process)
    else:
        run_parameters = None

    with open(output_path, 'w') as out:
        out.write(jinja_env.get_template('run_emails.html').render(
            run_id=runid, lane_data=lane_data, lane_header=lane_header,
            run_parameters=run_parameters,
            project_datas=project_datas
            ).encode('utf-8'))


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

