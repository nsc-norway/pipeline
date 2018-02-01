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

    fname = delivery_dir + "/Summary_email_for_NSC_" + run_id + ".xls"
    patterned = instrument_type in ["hiseqx", "hiseq4k"]

    fname_html = delivery_dir + "/Emails_for_" + run_id + ".html"
    template_dir = os.path.join(os.path.dirname(__file__), "template")
    if select_autoescape is None:
        jinja_env = Environment(loader=FileSystemLoader(template_dir))
    else:
        jinja_env = Environment(loader=FileSystemLoader(template_dir), autoescape=select_autoescape(['html','xml']))
    write_html_file(jinja_env, process, fname_html, run_id, projects, instrument_type.startswith('hiseq'),
            lane_stats, patterned)


def get_lane_summary_data(projects, print_lane_number, lane_stats, patterned):
    """This function gets per-lane statistics based on a provided lane_stats object and 
    projects. projects is the normal project list, with each project containing a list
    of samples, then files. This function will reconstruct some stats, like Q30, from the file-based
    stats. While this is not the most direct way, it is very portable since it only relies
    on bcl2fastq output (portable in the sense that it doesn't need LIMS, special handling
    of different sequencers, or other libraries)."""

    # A different table is used depending on the sequencer type: Does it have lanes? Does it have patterned FC?
    if print_lane_number:
        if patterned:
            header = ["Lane", "Project", "PF cluster no", "PF ratio", "SeqDuplicates", "Undetermined",
                    "AlignedPhiX", ">=Q30", "Quality"]
        else:
            header = ["Lane", "Project", "PF cluster no", "PF ratio", "Raw cluster density(/mm2)",
                    "PF cluster density(/mm2)", "Undetermined", "AlignedPhiX", ">=Q30", "Quality"]
    else:
        header = ["Project", "PF cluster no", "PF ratio", "Raw cluster density(/mm2)",
                "PF cluster density(/mm2)", "Undetermined", "AlignedPhiX",
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
    """This class gets and holds information about a project. It will query the LIMS for 
    contact information etc. if the lims_project argument is provided (not None). Its primary
    purpose is to gather the number of fragments in each of the files, and output it in a 
    list:
    file_fragment_table = [("Filename1", "100,000"), ("Filename2", "120,000"), ...]

    For diagnostics project it will "censor" the sample names, and only output one line
    per pair of reads (if paired end sequencing).
    """
    def __init__(self, project, lims_project, seq_process):
        self.nsamples = len(project.samples)
        self.name = project.name
        self.file_fragments_table = []
        self.dir = project.proj_dir
        diag_project= project.name.startswith("Diag-") or (
                lims_project and
                lims_project.udf.get('Project type') == "Diagnostics"
                )
        files = sorted(
                ((s,fi) for s in project.samples for fi in s.files),
                key=lambda (s,f): (f.lane, s.sample_index, f.i_read)
                )
        diag_sample_counter = 1
        for s,f in files:
            if diag_project:
                if f.i_read == 1:
                    sample = "Sample {0}".format(diag_sample_counter)
                    diag_sample_counter += 1
                else:
                    continue # Don't output for read 2
            else:
                sample = os.path.basename(f.path)
            if f.empty:
                self.file_fragments_table.append((sample, "0"))
            else:
                self.file_fragments_table.append((sample, utilities.display_int(f.stats['# Reads PF'])))
        if lims_project:
            self.lims = LimsInfo(lims_project, seq_process)


class RunParameters(object):
    """Get run parameters (LIMS based). Gets the information from the sequencing
    step."""

    def __init__(self, run_id, process):
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
    """Gets project information: contact person, etc., from UDFs in LIMS.
    Also identifies previous sequencing runs, and counts the number of lanes
    which are either PASSED, FAILED, or unknown."""
    def __init__(self, lims_project, seq_process):
        self.contact_person = lims_project.udf.get('Contact person')
        self.contact_email = lims_project.udf.get('Contact email')
        self.delivery_method = lims_project.udf.get('Delivery method')
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

