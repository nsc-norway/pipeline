import sys
import os
import re
import operator
import shutil
from math import ceil
from jinja2 import Environment, FileSystemLoader
try:
    from jinja2 import select_autoescape
except ImportError:
    select_autoescape = None

from common import nsc, stats, utilities, lane_info, samples, taskmgr

if nsc.TAG == "prod":
    from common import secure
else:
    sys.stderr.write("Using dummy security module\n")
    from common import secure_dummy as secure

TASK_NAME = "60. Emails"
TASK_DESCRIPTION = """Produce delivery reports for emails."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'lanes']


# Generate reports for emails based on demultiplexing stats


def main(task):
    task.running()
    
    run_id = task.run_id
    work_dir = task.work_dir
    instrument = utilities.get_instrument_by_runid(run_id)
    
    qc_dir = os.path.join(task.bc_dir, "QualityControl" + task.suffix)
    try:
        os.makedirs(os.path.join(qc_dir, "Delivery", "email_content"))
    except OSError:
        pass

    try:
        lane_stats = lane_info.get_from_interop(task.work_dir, task.no_lane_splitting)
    except lane_info.NotSupportedException:
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
        stats.add_duplication_results(qc_dir, projects)
    samples.flag_empty_files(projects, work_dir)

    delivery_dir = os.path.join(qc_dir, "Delivery")

    for project in projects:
        if not project.is_undetermined:
            fname = delivery_dir + "/Email_for_" + project.name + ".xls"
            write_sample_info_table(fname, run_id, project)

    patterned = instrument in ["hiseqx", "hiseq4k"]
    software_versions = [
            ("RTA", utilities.get_rta_version(work_dir))
            ]
    try:
        bcl2fastq_version = utilities.get_bcl2fastq2_version(task.process, work_dir)
        software_versions.append(("bcl2fastq", bcl2fastq_version))
    except RuntimeError:
        pass

    template_dir = os.path.join(os.path.dirname(__file__), "template")
    if select_autoescape is None:
        jinja_env = Environment(loader=FileSystemLoader(template_dir))
    else:
        jinja_env = Environment(loader=FileSystemLoader(template_dir),
                autoescape=select_autoescape(['html','xml']))
    write_html_and_email_files(jinja_env, task.process, task.bc_dir, delivery_dir, run_id, projects,
            instrument.startswith('hiseq'),
            lane_stats, software_versions, patterned)

    task.success_finish()


def get_lane_summary_data(projects, print_lane_number, lane_stats, patterned):
    """This function gets per-lane statistics based on a provided lane_stats object and 
    projects. projects is the normal project list, with each project containing a list
    of samples, then files. This function will reconstruct some stats, like Q30, from the file-based
    stats. While this is not the most direct way, it is very portable since it only relies
    on bcl2fastq output (portable in the sense that it doesn't need LIMS, special handling
    of different sequencers, or other libraries).
    
    For now it returns formatted strings for numeric values. This makes the logic in the
    template very simple, but we should consider moving the formatting to the template."""

    # A different table is used depending on the sequencer type: Does it have lanes? Does it have patterned FC?
    if print_lane_number:
        if patterned:
            header = ["Lane", "Project", "PF cluster no", "PF ratio", "SeqDuplicates", "Undetermined",
                    "AlignedPhiX", ">=Q30", "MaxReadsSam", "MinReadsSam", "Quality"]
        else:
            header = ["Lane", "Project", "PF cluster no", "PF ratio", "Raw cluster density(/mm2)",
                    "PF cluster density(/mm2)", "Undetermined", "AlignedPhiX", ">=Q30", 
                    "MaxReadsSam", "MinReadsSam", "Quality"]
    else:
        header = ["Project", "PF cluster no", "PF ratio", "Raw cluster density(/mm2)",
                "PF cluster density(/mm2)", "Undetermined", "AlignedPhiX",
                ">=Q30", "MaxReadsSam", "MinReadsSam", "Quality"]

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

        all_lane_files = [f
                for proj in projects
                for s in proj.samples
                for f in s.files
                if f.lane == l]
        all_nonempty_files = [f for f in all_lane_files if not f.empty]

        row = []
        proj = lane_proj[l]
        undetermined_file = lane_undetermined.get(l)

        if print_lane_number:
            row.append((l, "center"))
        row.append((proj.name, "text"))

        cluster_no = sum(f.stats['# Reads PF'] for f in all_nonempty_files if f.i_read == 1)
        row.append((utilities.display_int(cluster_no), "number"))
        lane = lane_stats[l]
        row.append(("%4.2f" % (lane.pf_ratio if lane.pf_ratio is not None else 0.0), "number"))
        if not patterned:
            row.append((utilities.display_int(lane.cluster_den_raw), "number"))
            row.append((utilities.display_int(lane.cluster_den_pf), "number"))

        if patterned:
            dupsum = sum(f.stats['fastdup reads with duplicate']
                    for f in all_nonempty_files if f.i_read == 1)
            try:
                duppct = dupsum * 100.0 / cluster_no
                row.append(("%4.2f %%" % duppct, "number"))
            except ZeroDivisionError:
                row.append(("-", "center"))

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

        norm = sum(f.stats['# Reads PF'] for f in all_lane_files if not f.empty)
        q30sum = sum(f.stats['% Bases >=Q30']*f.stats['# Reads PF'] for f in all_nonempty_files)
        q30pct = q30sum * 1.0 / max(norm, 1)
        row.append(("%4.2f%%" % q30pct, "number"))

        all_sample_reads = [
                f.stats['# Reads PF'] if not f.empty else 0
                for proj in projects
                for s in proj.samples
                for f in s.files
                if f.lane == l and not proj.is_undetermined
                ]
        sample_norm = sum(all_sample_reads)
        if sample_norm > 0:
            min_reads = min(all_sample_reads)
            max_reads = max(all_sample_reads)
            mean_reads = sample_norm * 1.0 / len(all_sample_reads)

            row.append(("%+3.1f%%" % ((max_reads - mean_reads) * 100.0 / mean_reads), "number"))
            row.append(("%+3.1f%%" % ((min_reads - mean_reads) * 100.0 / mean_reads), "number"))
        else:
            row.append(("-", "number"))
            row.append(("-", "number"))

        row.append(("ok", "text"))
        data.append(row)
    return header, data


class ProjectData(object):
    """This class gets and holds information about a project. It will query the LIMS for 
    contact information etc. if the lims_project argument is provided (not None). Its primary
    purpose is to gather the number of fragments in each of the files, and output it in a 
    list. For each file, it writes a tuple of the file name, the number of fragments, and the
    relative difference from the mean number of reads (see below).

    file_fragment_table = [("Filename1", 100000, -0.09), ("Filename2", 120000, 0.09), ...]

    The relative difference is: (frags - mean_frags) / mean_frags

    For diagnostics projects it will censor the sample names, and only output one line
    per pair of reads (if paired end sequencing).
    """
    def __init__(self, project, lims_project, seq_process):
        self.project = project
        self.nsamples = len(project.samples)
        self.name = project.name
        self.file_fragments_table = []
        self.dir = project.proj_dir
        self.diag_project= project.name.startswith("Diag-") or (
                lims_project and
                lims_project.udf.get('Project type') == "Diagnostics"
                )
        files = sorted(
                ((s,fi) for s in project.samples for fi in s.files),
                key=lambda (s,f): (f.lane, s.sample_index, f.i_read)
                )

        mean_frags = {}
        for lane in set(f.lane for s, f in files):
            num_frags = [
                    f.stats['# Reads PF'] if not f.empty else 0
                    for s,f in files
                    if f.lane == lane
                    ]
            sum_frags = sum(num_frags)
            if sum_frags > 0: # Mean fragments over files. Only used if there are any samples.
                mean_frags[lane] = sum_frags * 1.0 / len(num_frags)

        diag_sample_counter = 1
        for s,f in files:
            if self.diag_project:
                if f.i_read == 1:
                    sample = "Sample {0}".format(diag_sample_counter)
                    diag_sample_counter += 1
                else:
                    continue # Don't output for read 2
            else:
                sample = os.path.basename(f.path)
            if f.empty:
                self.file_fragments_table.append((sample, 0, -1.0 if mean_frags > 0 else 0.0))
            else:
                self.file_fragments_table.append(
                        (sample, f.stats['# Reads PF'],
                            (f.stats['# Reads PF'] - mean_frags[f.lane]) * 1.0 / mean_frags[f.lane])
                        )
        if lims_project:
            self.lims = utilities.LimsInfo(lims_project, seq_process)
        else:
            self.lims = None


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


def get_data_size(bc_dir, project):
    size = 0
    for sample in project.samples:
        for file in sample.files:
            if not file.empty:
                size += os.path.getsize(os.path.join(bc_dir, file.path))
    return size


def write_html_and_email_files(jinja_env, process, bc_dir, delivery_dir, run_id, projects, print_lane_number,
        lane_stats, software_versions, patterned):
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
        run_parameters = RunParameters(run_id, seq_process)
    else:
        run_parameters = None

    with open(delivery_dir + "/Emails_for_" + run_id + ".html", 'w') as out:
        doc_content = jinja_env.get_template('run_emails.html').render(
                                run_id=run_id, lane_data=lane_data, lane_header=lane_header,
                                run_parameters=run_parameters, software_versions=software_versions,
                                project_datas=project_datas
                                )
        doc_bytes = doc_content.encode('utf-8') 
        out.write(doc_bytes)


    # Summary file for email content
    summary_file_path = delivery_dir + "/email_content/Summary_for_" + run_id + ".html"
    with open(summary_file_path, 'w') as out:
        doc_content = jinja_env.get_template('run_summary.html').render(
                                lane_data=lane_data, lane_header=lane_header,
                                run_parameters=run_parameters, software_versions=software_versions,
                                project_datas=project_datas
                                )
        doc_bytes = doc_content.encode('utf-8') 
        out.write(doc_bytes)

    # Per-project file for email content
    for project_data in project_datas:
        with open(delivery_dir + "/email_content/" + project_data.dir + ".txt", 'w') as out:
            size = username = password = None
            if project_data.lims is None or project_data.lims.delivery_method == "User HDD":
                size = ceil(get_data_size(bc_dir, project_data.project) / 1024.0**3) + 1
            elif project_data.lims.delivery_method == "Norstore":
                match = re.match("^([^-]+)-([^-]+)-\d\d\d\d-\d\d-\d\d$", project_data.name)
                name = match.group(1)
                proj_type = match.group(2)
                username = name.lower() + "-" + proj_type.lower()
                password = secure.get_norstore_password(process, project_data.name)
            doc_content = jinja_env.get_template('project_email.txt').render(project_data=project_data,
                    username=username, password=password, size=size)
            doc_bytes = doc_content.encode('utf-8') 
            out.write(doc_bytes)

    # Make symlink to multiqc report
    for project_data in project_datas:
        link_placement = delivery_dir + "/email_content/{}_multiqc.html".format(project_data.dir)
        try:
            os.symlink("../../{}/multiqc_report.html".format(project_data.name), link_placement)
        except OSError as e:
            if e.errno == 17: pass # File exists
            else: raise

    # List of emails to send
    with open(delivery_dir + "/automatic_email_list.txt", 'w') as out:
        for e in get_email_recipient_info(run_id, project_datas):
            if not any(x is None for x in e):
                out.write("|".join(e) + "\n")

    script_file = os.path.join(delivery_dir, "Open_emails.command")
    if not os.path.exists(script_file):
        try:
            os.link(nsc.OPEN_EMAILS_SCRIPT, script_file)
        except OSError as e:
            if e.errno == 18: # Cross-device link
                shutil.copyfile(nsc.OPEN_EMAILS_SCRIPT, script_file)
            else:
                pass # Missing script file is not an error
            

def get_email_recipient_info(run_id, project_datas):
    summary_recipients = set(('nsc-ous-data-delivery@sequencing.uio.no',))
    emails = []
    for project_data in project_datas:
        if project_data.lims:
            email_to = project_data.lims.contact_email
        else:
            email_to = ""
        if project_data.diag_project:
            email_to = "diag-lab@medisin.uio.no,diag-bioinf@medisin.uio.no"
            summary_recipients.add('diag-lab@medisin.uio.no')
            summary_recipients.add('diag-bioinf@medisin.uio.no')
            if project_data.name.startswith("Diag-EKG"):
                summary_recipients.add('EKG-HTS@medisin.uio.no')
                email_to += ',EKG-HTS@medisin.uio.no'
            elif project_data.name.startswith("Diag-EHG"):
                summary_recipients.add('EHG-HTS@medisin.uio.no')
                email_to += ',EHG-HTS@medisin.uio.no'
        elif project_data.name.startswith("TI-") or project_data.name.startswith("MIK-"):
            summary_recipients.add(email_to)
        email_cc = ""
        email_bcc = "nsc-ous-data-delivery@sequencing.uio.no"
        email_subject = "Sequence ready for download - sequencing run {run_id} - {name} ({nsamples} samples)".format(
                run_id = run_id,
                name = project_data.name,
                nsamples = project_data.nsamples
                )
        email_content_file = "email_content/{}.txt".format(project_data.dir)
        if project_data.diag_project:
            email_attachment = ""
        else:
            email_attachment = "email_content/" + project_data.dir + "_multiqc.html"
        emails.append(("text", email_to, email_cc, email_bcc, email_subject, email_content_file, email_attachment))
    
    email_to = ",".join(summary_recipients)
    email_cc = ""
    email_bcc = ""
    email_subject = "Summary for run {run_id}".format(run_id=run_id)
    email_content_file = "email_content/Summary_for_{run_id}.html".format(run_id=run_id)
    email_attachment = ""
    emails.append(("html", email_to, email_cc, email_bcc, email_subject, email_content_file, email_attachment))
    return emails


def write_sample_info_table(output_path, runid, project):
    """Legacy "email" file for project read numbers"""
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



if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

