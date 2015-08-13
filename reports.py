import sys, os

from genologics.lims import *
from common import nsc, stats, utilities, lane_info, samples

# Generate reports after FastQC has completed


def main_lims(process_id):
    process = Process(nsc.lims, id=process_id)
    run_id = process.udf[nsc.RUN_ID_UDF]
    instument = utilities.get_instrument_from_runid(run_id)
    work_dir = utilities.get_udf(
            process, nsc.WORK_RUN_DIR_UDF,
            os.path.join(nsc.SECONDARY_STORAGE, run_id)
            )
    
    run_stats = get_run_stats(instrument, work_dir)
    projects = samples.get_projects_by_process(process)
    samples.add_stats(projects, run_stats)

    lane_stats = lane_info.get_from_lims(process, instrument)

    make_reports(work_dir, run_id, projects, lane_stats)


def main(work_dir):
    run_id = os.path.basename(os.path.realpath(run_dir))
    instrument = get_instrument_by_runid(run_id)
    
    lane_stats = lane_info.get_from_files(work_dir, instrument)

    data_reads, index_reads = utilities.get_num_reads(work_dir)

    sample_sheet_path = os.path.join(work_dir, "DemultiplexingSampleSheet.csv")
    run_stats = get_run_stats(instrument, work_dir)
    projects = samples.get_projects_by_files(work_dir, sample_sheet_path)
    samples.add_stats(projects, run_stats)

    make_reports(work_dir, run_id, projects, lane_stats)


def get_run_stats(instrument, work_dir):
    if instrument == "nextseq":
        run_stats = stats.get_bcl2fastq_stats(
                os.path.join(work_dir, "Stats"),
                aggregate_lanes = True,
                aggregate_reads = False
                )
    else:
        run_stats = stats.get_bcl2fastq_stats(
                os.path.join(work_dir, "Stats"),
                aggregate_lanes = False,
                aggregate_reads = False
                )
    return run_stats


def make_reports(work_dir, run_id, projects, lane_stats):
    basecalls_dir = os.path.join(work_dir, "Data", "Intensities", "BaseCalls")
    qc_dir = os.path.join(basecalls_dir, "QualityControl")
    os.umask(007)

    for project in projects:
        if not project.is_undetermined:
            fname = delivery_dir + "/Email_for_" + project.name + ".xls"
            write_sample_info_table(fname, run_id, project)

    fname = delivery_dir + "/Table_for_GA_runs_" + run_id + ".xls"
    write_internal_sample_table(fname, run_id, projects, lane_stats)

    fname = delivery_dir + "/Summary_email_for_NSC_" + run_id + ".xls"
    instrument_type = utilities.get_instrument_by_runid(run_id)
    write_summary_email(fname, run_id, projects, instrument_type=='hiseq', lane_stats)


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


def qc_pdf_name(run_id, fastq):
    report_root_name = re.sub(".fastq.gz$", ".qc", os.path.basename(fastq.path))
    if fastq.lane.is_merged:
        return "{0}.{1}.pdf".format(run_id, report_root_name)
    else:
        return "{0}.{1}.{2}.pdf".format(run_id, fastq.lane.id, report_root_name)


def generate_report_for_customer(args):
    """Generate PDF report for a fastq file.

    The last argument, sample_fastq, is a tuple containing a 
    Sample object and a FastqFile object"""
    fastq_dir, quality_control_dir, run_id, software_versions, template,\
            sample, fastq = args
    sample_dir = os.path.join(quality_control_dir, "Sample_" + sample.name)
    pdf_dir = os.path.join(sample_dir, "pdf")
    try:
        os.mkdir(pdf_dir)
    except OSError:
        pass

    replacements = {
        '__RunName__': tex_escape(run_id),
        '__Programs__': tex_escape(" & ".join(v[0] for v in software_versions)),
        '__VersionString__': tex_escape(" & ".join(v[1] for v in software_versions)),
        '__SampleName__': tex_escape(sample.name),
        '__ReadNum__': str(fastq.read_num),
        '__TotalN__': utilities.display_int(fastq.stats['# Reads PF']),
        '__Folder__': '../' + fastqc_dir(fastq.path),
        '__TemplateDir__': template_dir
            }

    rootname = re.sub(".fastq.gz$", ".qc", os.path.basename(fastq.path))
    fname = rootname + ".tex"
    with open(pdf_dir + "/" + fname, "w") as of:
        of.write(replace_multiple(replacements, template))

    DEVNULL = open(os.devnull, 'wb') # discard output
    subprocess.check_call([nsc.PDFLATEX, '-shell-escape', fname], stdout=DEVNULL, stdin=DEVNULL, cwd=pdf_dir)

    orig_pdfname = rootname + ".pdf"
    pdfname = qc_pdf_name(run_id, fastq)
    shutil.copyfile(pdf_dir + "/" + orig_pdfname, os.path.join(fastq_dir, os.path.dirname(fastq.path), pdfname))
    os.rename(pdf_dir + "/" + orig_pdfname, quality_control_dir + "/" + pdfname)


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



def generate_internal_html_report(quality_control_dir, samples):
    # Generate the NSC QC report HTML file
    top_file = open(template_dir + "/QC_NSC_report_template.html")
    overrepresented_seq_buffer = ""
    shutil.copy(template_dir + "/NSC_logo_original_RGB.tif", quality_control_dir)
    with open(os.path.join(quality_control_dir, "NSC.QC.report.htm"), 'w') as out_file:
        out_file.write(top_file.read())
    
        images = ["per_base_quality.png", "per_base_sequence_content.png", "per_sequence_quality.png", "per_base_n_content.png", "duplication_levels.png"]
    
        i = 0
        samples_files = sorted(
                ((s,fi) for s in samples for fi in s.files),
                key=lambda (s,f): (f.lane.id, s.name, f.read_num)
                )
        for s, fq in samples_files:
            subdir = "Sample_" + s.name
            fq_name = os.path.basename(fq.path)
            fqc_dir = fastqc_dir(fq.path)

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
                    cell = "<td><a href=\"{subDir}/{fastqcDir}/Images/{image}\"><img src=\"{subDir}/{fastqcDir}/Images/{image}\" class=\"graph\" align=\"center\"/></a></td>\n"
                    out_file.write(cell.format(subDir=subdir, fastqcDir=fqc_dir, image=img))

            
            report_path = os.path.join(quality_control_dir, subdir, fqc_dir, "fastqc_report.html")
            if not fq.empty:
                celln = "<td align=\"center\"><b><a href=\"#M{index}\">Overrepresented sequences</a></b></td>\n</tr>\n";
                out_file.write(celln.format(subDir=subdir, fileName=fq_name, index=i))
                overrepresented_seq_buffer += extract_format_overrepresented(report_path, fq_name, "M" + str(i))
            else:
                out_file.write("<td></td>\n</tr>\n")

            i += 1

        out_file.write("</table>\n")
        out_file.write(overrepresented_seq_buffer)
        out_file.write("</div>\n</body>\n</html>\n")



if __name__ == "__main__":
    main_lims(sys.argv[1])

