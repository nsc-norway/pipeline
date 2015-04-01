# QC functions library module file

# This module provides QC-related functions for all Illumina sequencer types.

import subprocess
import re, os
import shutil
from multiprocessing import Pool
import nsc, utilities


def run_fastqc(files, output_dir=None, max_threads=None):
    '''Run fastqc on a set of fastq files'''
    args = []
    if max_threads:
        args += ['--threads=' + str(max_threads)]
    if output_dir:
        args += ["--outdir=" + output_dir]
    args += files
    print "Running fastqc on", len(files), "files"
    rc = subprocess.call([nsc.FASTQC] + args)
    


def fastqc_dir(fp):
    '''Get base name of fastqc directory given the name of the fastq file'''
    if not fp.endswith(".fastq.gz"):
        raise ValueError("Can only process fastq.gz files!")
    return re.sub(".fastq.gz$", "_fastqc", os.path.basename(fp))



def move_fastqc_results(quality_control_dir, sample):
    '''Move fastqc reports into correct subdirectories. Initially, 
    they will be created directly in the quality_control_dir.
    (The reason is that we want to run a single fastqc command, to 
    use multi-threading, so we can't give different output dirs)'''

    sample_dir = os.path.join(quality_control_dir, "Sample_" + sample.name)
    try:
        os.mkdir(sample_dir)
    except OSError:
        pass
    
    for f in sample.files:
        fp = f.path
        fastqc_result_dir_name = fastqc_dir(fp)

        # Don't need the zip, as we have the uncompressed version.
        os.remove(os.path.join(quality_control_dir, fastqc_result_dir_name + ".zip"))

        # Move from root fastqc to sample directory
        src_file = os.path.join(quality_control_dir, fastqc_result_dir_name)
        dst_file = os.path.join(sample_dir, fastqc_result_dir_name)
        os.rename(src_file, dst_file)


def replace_multiple(replacedict, string):
    '''Replace each key in replacedict found in string with the
    value in replacedict. Note: special regex characters are not
    escaped, as we don't need that.'''
    # thanks, stackoverflow 6116978 (also considered Python's Template, 
    # but the use of dollar signs in template placeholders interferes 
    # with latex syntax, though only when editing the template itself)

    pattern = re.compile('|'.join(replacedict.keys()))
    return pattern.sub(lambda m: rep[m.group(0)], text)


def tex_escape(s):
    return re.sub("(!?\d|\w|\(|\)|\+|\-|\.", lambda x: "\\" + x.group(0), s)


def generate_report_for_customer(quality_control_dir, run_id,
        software_versions, template, sample_fastq):
    '''Generate PDF report for a fastq file.

    The last argument, sample_fastq, is a tuple containing a 
    Sample object and a FastqFile object'''

    sample, fastq = sample_fastq
    sample_dir = os.path.join(quality_control_dir, "Sample_" + sample.name)
    pdf_dir = os.path.join(sample_dir, "pdf")
    os.mkdir(pdf_dir)
    os.chdir(pdf_dir) # fine to do cd in parallel since we're using multiprocessing

    raw_replacements = {
        '__RunName__': run_id,
        '__VersionString__': " & ".join([software_versions['RTA'], software_versions['bcl2fastq']]),
        '__SampleName__': sample.name,
        '__ReadNum__': fastq.read_num,
        '__TotalN__': sample.num_pf_reads,
        '__Folder__': '../' + fastqc_dir(fastq.path)
            }

    replacements = dict((k, tex_escape(v)) for k,v in raw_replacements.items())
    report_root_name = ".".join(run_id, str(fastq.lane.id), "Sample_" + sample.name,
            "Read" + str(fastq.read_num), "qc")
    fname = report_root_name + ".tex"
    of = open(fname, "w")
    of.write(replace_multiple(replacements, template))
    of.close()
    DEVNULL = open(os.devnull, 'wb') # discard output
    subprocess.check_call([nsc.PDFLATEX, '-shell-escape', fname],
            stdout=DEVNULL, stdin=DEVNULL)

    pdfname = report_root_name + ".pdf"
    fastq_sample_dir = os.path.dirname(fastq.path)
    os.copy(pdfname, fastq_sample_dir + "/" + pdfname)
    os.rename(pdfname, quality_control_dir + pdfname)


def compute_md5(project_path, threads):
    md5data = utilities.check_output([nsc.MD5DEEP, "-rl", "-j" + str(threads), "."],
            cwd=project_path)
    open(os.path.join(project_path, "md5sum.txt"), "w").write(md5data)


def extract_format_overrepresented(fqc_report, fastqfile, index):
    '''Processes the fastqc_report.html file for a single fastq file and extracts the
    overrepresented sequences.'''

    with open(fqc_report) as reportfile: # we'll be good about closing these, since there may be many
        found_over = False
        buf = ""

        for l in reportfile:
            if found_over:
                if "<table>" in l:
                    buf += '<table border="1">\n'
                elif "</table>" in l:
                    buf += l
                    break
                else:
                    buf += l

            elif "No overrepresented sequences" in l:
                return '''\
<h2 id="{id}">{laneName}</h2>
<div style="font:10pt courier">
<p>No overrepresented sequences</p>
<p></p>
</div>
'''.format(id=index, laneName=fastqfile)
            elif "Overrepresented sequences</h2>" in l:
                found_over = True
                buf += '<h2 id="{id}">{laneName}</h2>\n'.format(id=index, laneName=fastqfile)
                buf += '<p></p>\n'
                buf += '<div style="font: 10pt courier;">\n'

        buf += "</div>\n"
        return buf



def generate_internal_html_report(quality_control_dir, samples):
    # Generate the NSC QC report HTML file
    top_file = open(os.path.dirname(__file__) + "/template/QC_NSC_report_template.html")
    overrepresented_seq_buffer = ""
    shutil.copy(os.path.dirname(__file__) + "/template/NSC_logo_original_RGB.tif",
                            quality_control_dir)
    with open(os.path.join(quality_control_dir, "NSC.QC.report.htm"), 'w') as out_file:
        out_file.write(top_file.read())
    
        images = ["per_base_quality.png", "per_base_sequence_content.png", "per_sequence_quality.png", "per_base_n_content.png", "duplication_levels.png"]
    
        i = 0
        for s in samples:
            subdir = "Sample_" + s.name
            for fq in s.files:
                fq_name = os.path.basename(fq.path)
                fqc_dir = fastqc_dir(fq.path)

                cell1 = "<tr><td align=\"left\"><b>{fileName}<br><br>SampleName: {sampleName}<br>Read Num: {nReads}</b></td>\n"
                out_file.write(cell1.format(fileName=fq_name, sampleName=s.name, nReads=fq.num_pf_reads))
    
                for img in images:
                    cell = "<td><img src=\"{subDir}/{fastqcDir}/Images/{image}\" class=\"graph\" align=\"center\"></td>\n"
                    out_file.write(cell.format(subDir=subdir, fastqcDir=fqc_dir, image=img))
    
                celln = "<td align=\"center\"><b><a name=\"{subDir}/{fileName}\"><a href=\"{index}\">Overrepresented sequences</a></b></td>\n</tr>\n";
                out_file.write(celln.format(subDir=subdir, fileName=fq_name, index=i))
                
                report_path = os.path.join(quality_control_dir, subdir, fqc_dir, "fastqc_report.html")
                overrepresented_seq_buffer += extract_format_overrepresented(report_path, fq_name, index)

                i += 1
        out_file.write("</table>\n")
        out_file.write(overrepresented_seq_buffer)
        out_file.write("</div>\n</body>\n</html>\n")

            

def write_sample_info_table(output_path, runid, project):
    with open(output_path, 'w') as out:
        out.write('--------------------------------		\n')
        out.write('Email for ' + project.name)
        out.write('--------------------------------		\n\n')
        nsamples = len(project.samples)
        out.write('Sequence ready for download - sequencing run ' + runid + ' - ' + project.name + ' (' + nsamples + ' samples)\n\n')

        for f in (fi for fi in s.files for s in project.samples):
            out.write(os.path.basename(fi.path) + "\t")
            out.write(str(f.num_pf_reads) + "\t")
            out.write("fragments\n")



def write_internal_sample_table(output_path, runid, samples):
    with open(output_path, 'w') as out:
        out.write("--------------------------------\n")
        out.write("Table for GA_runs.xlsx\n")
        out.write("--------------------------------\n\n")
        out.write("Summary for run " + runid + "\n\n")
        out.write("Need not copy column A\n\n")

        for s in samples:
            for f in s.files:
                if f.read_num == 1:
                    out.write(s.name + "\t")
                    out.write(f.lane.raw_cluster_density + "\t")
                    out.write(f.lane.pf_cluster_density + "\t")
                    out.write(f.percent_of_pf_clusters + "%\t")
                    out.write(f.num_pf_reads + "\t")
                    out.write("\n")



def write_summary_email(output_path, runid, projects):
    with open(output_path, 'w') as out:
        summary_email_head = '''\
--------------------------------							
Summary email to NSC members							
--------------------------------							
							
Summary for run {runId}							
							
PF = pass illumina filter							
PF cluster no = number of PF cluster in the lane							
Undetermined ratio = how much proportion of fragments can not be assigned to a sample in the indexed lane							
Quality = summary of the overall quality							

Lane	Project	PF cluster no	PF ratio	Raw cluster density(/mm2)	PF cluster density(/mm2)	Undetermined ratio	Quality
'''.format(runId = runid)
        out.write(summary_email_head)
        # assumes 1 project per lane, and undetermined
        lane_proj = dict((proj.samples[0].files[0].lane.id, proj) for proj in projects if not re.match("lane\d", proj.name))
        lane_undetermined = dict((proj.samples[0].files[0].lane.id, proj) for proj in projects if re.match("lane\d", proj.name))

        for l in sorted(lane_proj.keys()):
            proj = lane_proj[l]
            undetermined = lane_undetermined[l]
            lane = proj.samples[0].files[0].lane
            out.write(str(l) + "\t")
            cluster_no = sum(f.num_pf_reads for f in s.files for s in proj.samples + undetermined.samples)
            out.write(str(cluster_no) + "\t")
            out.write(lane.pf_ratio + "\t")
            out.write(lane.raw_cluster_density + "\t")
            out.write(lane.pf_cluster_density + "\t")
            undetermined_ratio = undetermined.samples[-1].files[-1].percent_of_pf_clusters
            out.write(str(undetermined_ratio) + "%\t")
            out.write("ok\n")






def qc_main(demultiplex_dir, projects, run_id, software_versions, threads = 1):
    '''QC on demultiplexed data. Can be run per project, don't need
    access to all demultiplexed lanes.
    

    demultiplex_dir is the location of the demultiplexed reads,
    i.e., Unaligned.

    projects is a list of Project objects containing references
    to samples and files. See parse.py for Project, Sample and 
    FastqFile classes. This is a generalised specification of 
    the information in the sample sheet, valid for all Illumina
    instrument types. It also contains some data for the results, 
    not just experiment setup.

    software_versions is a dict with (software name: version)
    software name: RTA, bcl2fastq
    '''
    # Unaligned/inHouseDataProcessing/
    output_dir = os.path.join(demultiplex_dir, "inHouseDataProcessing")
    # Unaligned/inHouseDataProcessing/QualityControl
    quality_control_dir = os.path.join(output_dir, "QualityControl")
    delivery_dir = quality_control_dir + "/Delivery"
    for d in [output_dir, quality_control_dir, delivery_dir]:
        try:
            os.mkdir(d) 
        except OSError:
            pass

    print "Number of projects: ", len(projects)
    all_fastq = [f.path for pro in projects for s in pro.samples for f in s.files]

    if len(set(os.path.basename(f) for f in all_fastq)) < len(all_fastq):
        raise RuntimeError("Not all fastq file names are unique! Can't deal with this, consider splitting into smaller jobs.")

    # Run FastQC
    # First output all fastqc results into QualityControl, them move them
    # in place later
    run_fastqc(all_fastq, output_dir=quality_control_dir, max_threads=threads) 
    samples = [sam for pro in projects for sam in pro.samples]
    for s in samples:
       move_fastqc_results(quality_control_dir, s)

    # Generate PDF reports in parallel
    template = open(os.path.dirname(__file__) + "/template/QC_NSC_report_template.html").read()
    proc_closure = lambda x: generate_report_for_customer(
            quality_control_dir, run_id,
            software_versions, template, x)
    pool = Pool(int(threads))
    # Run one task for each fastq file, giving a sample reference and FastqFile as argument
    # along with the arguments specified in the lambda above
    pool.map(proc_closure, [(s,f) for s in samples for f in s.files])
    
    # Generate md5sums for projects
    for p in projects:
        compute_md5(p.path, threads)

    # Generate internal reports
    generate_internal_html_report(quality_control_dir, samples)
    
    # For email to customers
    for project in projects:
        fname = delivery_dir + "/Email_for_" + project.name + ".xls"
        write_sample_info_table(fname, run_id, project)

    # The following reports don't need to be used (i.e. read, copied) for  
    # LIMS-based runs, as a dedicated job will generate these for the full
    # run after all demultiplexing has completed.

    # Internal bookkeeping
    fname = delivery_dir + "/Table_for_GA_runs_" + run_id + ".xls"
    write_internal_sample_table(fname, run_id, samples)

    # Summary email for NSC staff
    fname = delivery_dir + "/Summary_email_for_NSC_" + run_id + ".xls"
    write_summary_email(fname, run_id, projects)


