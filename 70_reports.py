# Generate reports after FastQC has completed

# This task requires an intermediate amount of CPU resources. Will be 
# executed on the LIMS server, where we can make sure to have the necessary
# software installed.

import sys
import os
import re
import shutil
import multiprocessing
import subprocess
from xml.etree import ElementTree

from common import nsc, stats, samples, utilities, taskmgr

template_dir = os.path.dirname(__file__) + "/template"

TASK_NAME = "70. Reports"
TASK_DESCRIPTION = """Generates HTML and PDF reports based on demultiplexing stats
                    and FastQC results."""

TASK_ARGS = ['work_dir', 'sample_sheet']


def main(task):
    task.add_argument(
            '--bcl2fastq-version',
            default=None,
            help="bcl2fastq version to put in the reports"
            )
    task.running()
    run_id = task.run_id
    instument = utilities.get_instrument_by_runid(run_id)
    work_dir = task.work_dir
    bc_dir = task.bc_dir
    run_stats = stats.get_bcl2fastq_stats(
                os.path.join(bc_dir, "Stats"),
                aggregate_lanes = task.no_lane_splitting,
                aggregate_reads = False
                )
    projects = task.projects
    samples.add_stats(projects, run_stats)
    samples.flag_empty_files(projects, work_dir)

    if task.args.bcl2fastq_version:
        bcl2fastq_version = task.args.bcl2fastq_version
    elif task.process:
        bcl2fastq_version = utilities.get_udf(process, nsc.BCL2FASTQ_VERSION_UDF, None)
    else:
        bcl2fastq_version = get_bcl2fastq2_version(work_dir)
        if not bcl2fastq_version:
            task.fail("bcl2fastq version cannot be detected, use the --bcl2fastq-version option to specify!")

    make_reports(work_dir, run_id, projects, bcl2fastq_version)

    task.success_finish()


def get_bcl2fastq2_version(work_dir):
    """Check version in log file in standard location (for non-LIMS).
    
    Less than bullet proof way to get bcl2fastq2 version."""

    log_path = os.path.join(
            work_dir,
            nsc.RUN_LOG_DIR,
            "demultiplexing.bcl2fastq2.txt"
            )
    log = open(log_path)
    for i in xrange(3):
        l = next(log)
        if l.startswith("bcl2fastq v"):
            return l.split(" ")[1].strip("\n")

    else:
        return None
    

def get_rta_version(run_dir):
    try:
        xmltree = ElementTree.parse(os.path.join(run_dir, 'RunParameters.xml'))
    except IOError:
        xmltree = ElementTree.parse(os.path.join(run_dir, 'runParameters.xml'))

    run_parameters = xmltree.getroot()
    rta_ver = run_parameters.find("RTAVersion").text
    return rta_ver


def make_reports(work_dir, run_id, projects, bcl2fastq_version=None):
    basecalls_dir = os.path.join(work_dir, "Data", "Intensities", "BaseCalls")
    quality_control_dir = os.path.join(basecalls_dir, "QualityControl")

    software_versions = [("RTA", get_rta_version(work_dir))]
    if bcl2fastq_version:
        software_versions += [("bcl2fastq", bcl2fastq_version)]

    # Generate PDF reports in parallel
    # template_dir defined at top of file
    template = open(template_dir + "/reportTemplate_indLane_v4.tex").read()
    arg_pack = [basecalls_dir, quality_control_dir, run_id, software_versions, template]
    pool = multiprocessing.Pool()

    # PDF directory (all PDF files generated here)
    pdf_dir = os.path.join(quality_control_dir, "pdf")
    try:
        os.mkdir(pdf_dir)
    except OSError:
        pass

    # Run one task for each fastq file, giving a sample reference and FastqFile as argument 
    # as well as the ones given above. Debug note: change pool.map to map for better errors.
    map(
            generate_report_for_customer,
            [tuple(arg_pack + [p,s,f]) 
                for p in projects for s in p.samples for f in s.files
                if not f.empty
                ]
            )

    shutil.rmtree(pdf_dir)

    generate_internal_html_report(quality_control_dir, projects)



# PDF GENERATION CODE

def replace_multiple(replacedict, text):
    """Replace each key in replacedict found in string with the
    value in replacedict. Note: special regex characters are not
    escaped, as we don't need that."""
    # thanks, stackoverflow 6116978 (also considered Python's Template, 
    # but the use of dollar signs in template placeholders interferes 
    # with latex syntax, though only when editing the template itself)

    pattern = re.compile('|'.join(replacedict.keys()))
    return pattern.sub(lambda m: replacedict[m.group(0)], text)


def tex_escape(s):
    return re.sub(r"[^\d:a-zA-Z()+-. ]", lambda x: '\\' + x.group(0), s)


def generate_report_for_customer(args):
    """Generate PDF report for a fastq file.

    The last argument, sample_fastq, is a tuple containing a 
    Sample object and a FastqFile object"""
    fastq_dir, quality_control_dir, run_id, software_versions, template,\
            project, sample, fastq = args

    pdf_dir = os.path.join(quality_control_dir, "pdf")

    if sample.name:
        sample_name = sample.name
    else:
        sample_name = "Undetermined"

    replacements = {
        '__RunName__': tex_escape(run_id),
        '__Programs__': tex_escape(" & ".join(v[0] for v in software_versions)),
        '__VersionString__': tex_escape(" & ".join(v[1] for v in software_versions)),
        '__SampleName__': tex_escape(sample_name),
        '__ReadNum__': str(fastq.i_read),
        '__TotalN__': utilities.display_int(fastq.stats['# Reads PF']),
        '__Folder__': '../' + samples.get_fastqc_dir(project, sample, fastq),
        '__TemplateDir__': template_dir
            }

    rootname = re.sub(".fastq.gz$", ".qc", os.path.basename(fastq.path))
    fname = rootname + ".tex"
    with open(pdf_dir + "/" + fname, "w") as of:
        of.write(replace_multiple(replacements, template))

    DEVNULL = open(os.devnull, 'wb') # discard output
    subprocess.check_call([nsc.PDFLATEX, '-shell-escape', fname], stdout=DEVNULL, stdin=DEVNULL, cwd=pdf_dir)

    orig_pdfname = rootname + ".pdf"
    pdfname = samples.qc_pdf_name(run_id, fastq)
    os.rename(pdf_dir + "/" + orig_pdfname, os.path.join(fastq_dir, os.path.dirname(fastq.path), pdfname))



# HTML GENERATION
def extract_format_overrepresented(fqc_report, fastqfile, index):
    """Processes the fastqc_report.html file for a single fastq file and extracts the
    overrepresented sequences.
    
    Index is an arbitrary identifier used as an anchor (<a>) in the HTML."""

    with open(fqc_report) as reportfile: # we'll be good about closing these, since there may be many
        found_over = False
        buf = ""

        data = reportfile.read()
        if "No overrepresented sequences" in data:
            return """\
<h2 id="{id}">{laneName}</h2>
<div style="font:10pt courier">
<p>No overrepresented sequences</p>
<p></p>
</div>
""".format(id=index, laneName=fastqfile)
        else:
            body_m = re.search("Overrepresented sequences</h2>.*?<table>(.*?)</table>", data, re.DOTALL)
            if body_m:
                buf += '<h2 id="{id}">{laneName}</h2>\n'.format(id=index, laneName=fastqfile)
                buf += '<p></p>\n'
                buf += '<div style="font: 10pt courier;">\n'
                buf += '<table border="1">\n'
                buf += body_m.group(1)
                buf += "</table></div>\n"

        return buf



def generate_internal_html_report(quality_control_dir, projects):
    # Generate the NSC QC report HTML file
    top_file = open(template_dir + "/QC_NSC_report_template.html")
    overrepresented_seq_buffer = ""
    shutil.copy(template_dir + "/NSC_logo_original_RGB.tif", quality_control_dir)
    with open(os.path.join(quality_control_dir, "NSC.QC.report.htm"), 'w') as out_file:
        out_file.write(top_file.read())
    
        images = ["per_base_quality.png", "per_base_sequence_content.png", "per_sequence_quality.png", "per_base_n_content.png", "duplication_levels.png"]
    
        i = 0
        samples_files = sorted(
                ((p, s, fi) for p in projects for s in p.samples for fi in s.files),
                key=lambda (p, s,f): (f.lane, s.name, f.i_read)
                )
        for p, s, fq in samples_files:
            fq_name = os.path.basename(fq.path)
            fqc_dir = samples.get_fastqc_dir(p, s, fq)

            cell1 = "<tr><td align=\"left\"><b>{fileName}<br/><br/>SampleName: {sampleName}<br/>Read Num: {nReads}</b></td>\n"
            if fq.empty:
                n_reads = 0
            else:
                n_reads = fq.stats['# Reads PF']
            out_file.write(cell1.format(
                fileName=fq_name,
                sampleName=s.name,
                nReads=utilities.display_int(n_reads)
                ))

            for img in images:
                if fq.empty:
                    out_file.write("<td></td>\n")
                else:
                    cell = "<td><a href=\"{fastqcDir}/Images/{image}\"><img src=\"{fastqcDir}/Images/{image}\" class=\"graph\" align=\"center\"/></a></td>\n"
                    out_file.write(cell.format(fastqcDir=fqc_dir, image=img))

            
            report_path = os.path.join(quality_control_dir, fqc_dir, "fastqc_report.html")
            if not fq.empty:
                celln = "<td align=\"center\"><b><a href=\"#M{index}\">Overrepresented sequences</a></b></td>\n</tr>\n";
                out_file.write(celln.format(index=i))
                overrepresented_seq_buffer += extract_format_overrepresented(report_path, fq_name, "M" + str(i))
            else:
                out_file.write("<td></td>\n</tr>\n")

            i += 1

        out_file.write("</table>\n")
        out_file.write(overrepresented_seq_buffer)
        out_file.write("</div>\n</body>\n</html>\n")


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)


