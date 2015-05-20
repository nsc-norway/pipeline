# QC functions library module file

# This module provides QC-related functions for all Illumina sequencer types.

import subprocess
import re
import os
import shutil
import gzip
import operator
from multiprocessing import Pool
import nsc, utilities

template_dir = os.path.dirname(os.path.dirname(__file__)) + "/template"

# Model: Objects containing projects, samples and files.
# Represents a unified interface to the QC functionality, similar to the
# SampleInformation.txt BarcodeLaneStatistics.txt from the perl scripts.

class Project(object):
    """Project object.
    name: name with special characters replaced
    path: base name of project directory 
      path is relative to Unaligned for HiSeq, and relative to Data/Intensities/BaseCalls
      for MiSeq and NextSeq
    samples: list of samples
    """
    def __init__(self, name, proj_dir, samples=[], is_undetermined=False):
        self.name = name
        self.proj_dir = proj_dir
        self.samples = samples
        self.is_undetermined = is_undetermined


class Sample(object):
    """Contains information about a sample. Contains a list of FastqFile
    objects representing the reads. One instance for each sample on a 
    flowcell, even if that sample is run on multiple lanes."""

    def __init__(self, name, files):
        self.name = name
        self.files = files


class Lane(object):
    """Represents a lane (physically separate sub-units of flowcells).
    
    For MiSeq and NextSeq there is only one lane (NextSeq's lanes aren't
    independent).."""

    def __init__(self, id, raw_cluster_density, pf_cluster_density, pf_ratio):
        self.id = id
        self.raw_cluster_density = raw_cluster_density
        self.pf_cluster_density = pf_cluster_density
        self.pf_ratio = pf_ratio


class FastqFile(object):
    """Represents a single output file for a specific sample, lane and
    read. Currently assumed to be no more than one FastqFile per read.
    
    lane is a Lane object containing lane-specific stats.

    read_num is the read index, 1 or 2 (second read is only available for paired
    end). Index reads are not considered.

    Path is the path to the fastq file relative to the "Unaligned"
    (bcl2fastq output) directory or Data/Intensities/BaseCalls.
    
    stats is a dict of stat name => value. See the functions which generate these
    stats below.
    
    empty is set by the QC function. You may set it, but it will be overwritten."""

    def __init__(self, lane, read_num, path, stats):
        self.lane = lane
        self.read_num = read_num
        self.path = path
        self.stats = stats
        self.empty = False



############### FASTQC ################
def run_fastqc(files, demultiplex_dir, output_dir=None, max_threads=None):
    """Run fastqc on a set of fastq files"""
    args = []
    if max_threads:
        args += ['--threads=' + str(max_threads), '--quiet']
    if output_dir:
        args += ["--outdir=" + output_dir]
    args += files
    if len(files) > 0:
        print "Running fastqc on", len(files), "files"
        DEVNULL = open(os.devnull, 'wb') # discard output
        rc = subprocess.call([nsc.FASTQC] + args, stdout=DEVNULL, cwd=demultiplex_dir)
    else:
        print "No files provided for fastqc"
    


def fastqc_dir(fp):
    """Get base name of fastqc directory given the name of the fastq file"""
    if not fp.endswith(".fastq.gz"):
        raise ValueError("Can only process fastq.gz files!")
    return re.sub(".fastq.gz$", "_fastqc", os.path.basename(fp))



def move_fastqc_results(quality_control_dir, sample):
    """Move fastqc reports into correct subdirectories. Initially, 
    they will be created directly in the quality_control_dir.
    (The reason is that we want to run a single fastqc command, to 
    use multi-threading, so we can't give different output dirs)"""

    sample_dir = os.path.join(quality_control_dir, "Sample_" + sample.name)
    try:
        os.mkdir(sample_dir)
    except OSError:
        pass
    
    for f in sample.files:
        if not f.empty:
            fp = f.path
            fastqc_result_dir_name = fastqc_dir(fp)
            
            fastqc_output = os.path.join(quality_control_dir, fastqc_result_dir_name)
            # Don't need the zip, as we have the uncompressed version.
            if os.path.exists(fastqc_output):
                try:
                    os.remove(os.path.join(quality_control_dir, fastqc_result_dir_name + ".zip"))
                except OSError:
                    pass
        
                # Move from root fastqc to sample directory
                dst_file = os.path.join(sample_dir, fastqc_result_dir_name)
                if os.path.exists(dst_file):
                    shutil.rmtree(dst_file)
                os.rename(fastqc_output, dst_file)
            else:
                print "WARNING: No fastqc output found for", f.path
            

def update_stats_fastqc(quality_control_dir, sample):
    """Gets total number of sequences from fastqc output, or confirms existing
    number if already set"""

    sample_dir = os.path.join(quality_control_dir, "Sample_" + sample.name)
    for f in sample.files:
        if not f.empty:
            fastqc_result_dir = os.path.join(sample_dir, fastqc_dir(f.path))
            with open(fastqc_result_dir + "/fastqc_data.txt") as fqc_data:
                for l in fqc_data:
                    total = re.match("Total Sequences\t(\d+)", l)
                    if total:
                        if f.stats and f.stats.get('# Reads PF'):
                            assert f.stats['# Reads PF'] == int(total.group(1)),\
                                "file: %s: reads from fastqc: %d, reads from stats: %d" % (
                                            f.path, int(total.group(1)), f.stats['# Reads PF']
                                            )
                        else:
                            if not f.stats:
                                f.stats = {}
                            f.stats['# Reads PF'] = int(total.group(1))
                        break



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

    #report_root_name = ".".join((run_id, str(fastq.lane.id), "Sample_" + sample.name,
    #        "Read" + str(fastq.read_num), "qc"))
    report_root_name = re.sub(".fastq.gz$", ".qc", os.path.basename(fastq.path))
    fname = report_root_name + ".tex"
    with open(pdf_dir + "/" + fname, "w") as of:
        of.write(replace_multiple(replacements, template))

    DEVNULL = open(os.devnull, 'wb') # discard output
    subprocess.check_call([nsc.PDFLATEX, '-shell-escape', fname], stdout=DEVNULL, stdin=DEVNULL, cwd=pdf_dir)

    pdfname = report_root_name + ".pdf"
    shutil.copyfile(pdf_dir + "/" + pdfname, os.path.join(fastq_dir, os.path.dirname(fastq.path), pdfname))
    os.rename(pdf_dir + "/" + pdfname, quality_control_dir + "/" + pdfname)


def compute_md5(proj_dir, threads, files):
    md5data = utilities.check_output([nsc.MD5DEEP, "-rl", "-j" + str(threads)] + files,
            cwd=proj_dir)
    open(os.path.join(proj_dir, "md5sum.txt"), "w").write(md5data)


def extract_format_overrepresented(fqc_report, fastqfile, index):
    """Processes the fastqc_report.html file for a single fastq file and extracts the
    overrepresented sequences.
    
    Index is an arbitrary identifier used as an anchor (<a>) in the HTML."""

    with open(fqc_report) as reportfile: # we'll be good about closing these, since there may be many
        found_over = False
        buf = ""

        for l in reportfile:
            if "No overrepresented sequences" in l:
                return """\
<h2 id="{id}">{laneName}</h2>
<div style="font:10pt courier">
<p>No overrepresented sequences</p>
<p></p>
</div>
""".format(id=index, laneName=fastqfile)
            elif found_over:
                if "<table>" in l:
                    buf += '<table border="1">\n'
                elif "</table>" in l:
                    buf += l
                    break
                else:
                    buf += l
            elif "Overrepresented sequences</h2>" in l:
                found_over = True
                buf += '<h2 id="{id}">{laneName}</h2>\n'.format(id=index, laneName=fastqfile)
                buf += '<p></p>\n'
                buf += '<div style="font: 10pt courier;">\n'

        buf += "</div>\n"
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

            

def write_sample_info_table(output_path, runid, project):
    with open(output_path, 'w') as out:
        out.write('--------------------------------		\n')
        out.write('Email for ' + project.name + "\n")
        out.write('--------------------------------		\n\n')
        nsamples = len(project.samples)
        out.write('Sequence ready for download - sequencing run ' + runid + ' - ' + project.name + ' (' + str(nsamples) + ' samples)\n\n')

        files = sorted(
                ((s,fi) for s in project.samples for fi in s.files),
                key=lambda (s,f): (f.lane.id, s.name, f.read_num)
                )
        for s,f in files:
            out.write(os.path.basename(f.path) + "\t")
            if f.empty:
                out.write("0\t")
            else:
                out.write(utilities.display_int(f.stats['# Reads PF']) + "\t")
            out.write("fragments\n")



def write_internal_sample_table(output_path, runid, projects):
    with open(output_path, 'w') as out:
        out.write("--------------------------------\n")
        out.write("Table for GA_runs.xlsx\n")
        out.write("--------------------------------\n\n")
        out.write("Summary for run " + runid + "\n\n")
        out.write("Need not copy column A\n\n")
        samples = sorted(
                (s
                for proj in projects
                for s in proj.samples
                if proj.name != "Undetermined_indices"),
                key=operator.attrgetter('name')
                )
        for s in samples:
            for f in s.files:
                if f.read_num == 1:
                    out.write(s.name + "\t")
                    out.write(utilities.display_int(f.lane.raw_cluster_density) + "\t")
                    out.write(utilities.display_int(f.lane.pf_cluster_density) + "\t")
                    if f.empty:
                        out.write("%4.2f" % (0,) + "%\t")
                        out.write("0\t")
                    else:
                        out.write("%4.2f" % (f.stats.get('% of PF Clusters Per Lane', 0)) + "%\t")
                        out.write(utilities.display_int(f.stats['# Reads PF']) + "\t")
                    out.write("ok\t\tok\n")



def write_summary_email(output_path, runid, projects, print_lane_number):
    with open(output_path, 'w') as out:
        summary_email_head = """\
--------------------------------							
Summary email to NSC members							
--------------------------------							
							
Summary for run {runId}							
							
PF = pass illumina filter							
PF cluster no = number of PF cluster in the lane							
Undetermined ratio = how much proportion of fragments can not be assigned to a sample in the indexed lane							
Quality = summary of the overall quality							
""".format(runId = runid)
        if print_lane_number:
            summary_email_head += """
Lane	Project	PF cluster no	PF ratio	Raw cluster density(/mm2)	PF cluster density(/mm2)	Undetermined ratio	Quality
"""
        else:
            summary_email_head += """
Project	PF cluster no	PF ratio	Raw cluster density(/mm2)	PF cluster density(/mm2)	Undetermined ratio	Quality
"""

        out.write(summary_email_head)
        # assumes 1 project per lane, and undetermined
        # Dict: lane ID => (lane object, project object)
        lane_proj = dict((f.lane.id, (f.lane, proj)) for proj in projects
                for s in proj.samples for f in s.files if proj.name != "Undetermined_indices")
        # lane ID => file object (will be the last one)
        lane_undetermined = dict((f.lane.id, f) for proj in projects
                for s in proj.samples for f in s.files if proj.name == "Undetermined_indices")

        for l in sorted(lane_proj.keys()):
            lane, proj = lane_proj[l]
            try:
                undetermined_file = lane_undetermined[l]
            except KeyError:
                undetermined_file = None

            if print_lane_number:
                out.write(str(l) + "\t")
            out.write(proj.name + "\t")
            cluster_no = sum(f.stats['# Reads PF']
                    for proj in projects
                    for s in proj.samples
                    for f in s.files
                    if f.lane.id == l and f.read_num == 1 and not f.empty)
            out.write(utilities.display_int(cluster_no) + '\t')
            out.write("%4.2f" % (lane.pf_ratio) + "\t")
            out.write(utilities.display_int(lane.raw_cluster_density) + '\t')
            out.write(utilities.display_int(lane.pf_cluster_density) + '\t')
            if undetermined_file and\
                   not undetermined_file.empty and\
                   undetermined_file.stats.has_key('% of PF Clusters Per Lane'):
                out.write("%4.2f" % (undetermined_file.stats['% of PF Clusters Per Lane'],) + "%\t")
            else:
                out.write("-\t")
            out.write("ok\n")


def qc_main(input_demultiplex_dir, projects, instrument_type, run_id,
        software_versions, threads = 1):
    """QC on demultiplexed data. Can be run per project, don't need
    access to all demultiplexed lanes.

    input_demultiplex_dir is the location of the demultiplexed reads,
    i.e., Unaligned.

    projects is a list of Project objects containing references
    to samples and files. See above for Project, Sample and 
    FastqFile classes. This is a generalised specification of 
    the information in the sample sheet, valid for all Illumina
    instrument types. It also contains some data for the results, 
    not just experiment setup.

    software_versions is a dict with (software name: version)
    software name: RTA, bcl2fastq
    """
    os.umask(007)

    # Unaligned
    demultiplex_dir = os.path.abspath(input_demultiplex_dir)
    # Unaligned/QualityControl
    quality_control_dir = os.path.join(demultiplex_dir, "QualityControl")
    # Unaligned/QualityControl/Delivery
    delivery_dir = quality_control_dir + "/Delivery"
    for d in [quality_control_dir, delivery_dir]:
        try:
            os.mkdir(d) 
        except OSError:
            pass

    print "Number of projects: ", len(projects)
    all_fastq = []
    non_empty_files = []

    for f in [f for pro in projects for s in pro.samples for f in s.files]:
        all_fastq.append(f.path)
        gzf = gzip.open(os.path.join(demultiplex_dir, f.path))
        f.empty = len(gzf.read(1)) == 0
        gzf.close()
        if not f.empty:
            non_empty_files.append(f.path)

    if len(set(os.path.basename(f) for f in all_fastq)) < len(all_fastq):
        raise RuntimeError("Not all fastq file names are unique! Can't deal with this, consider splitting into smaller jobs.")

    # Run FastQC
    # First output all fastqc results into QualityControl, them move them
    # in place later
    run_fastqc(non_empty_files, demultiplex_dir, output_dir=quality_control_dir, max_threads=threads) 
    samples = [sam for pro in projects for sam in pro.samples]
    for s in samples:
       move_fastqc_results(quality_control_dir, s)
       # Get number of sequences. For (Mi|Next)Seq this is the only way to 
       # get this stat, for HiSeq this acts as a cross check.
       update_stats_fastqc(quality_control_dir, s)

    # Generate PDF reports in parallel
    template = open(template_dir + "/reportTemplate_indLane_v4.tex").read()
    arg_pack = [demultiplex_dir, quality_control_dir, run_id, software_versions, template]
    pool = Pool(int(threads))
    # Run one task for each fastq file, giving a sample reference and FastqFile as argument 
    # as well as the ones given above. Debug note: change pool.map to map for better errors.
    pool.map(generate_report_for_customer, [tuple(arg_pack + [s,f]) for s in samples for f in s.files if not f.empty]) 
    
    # Generate md5sums for projects
    for p in projects:
        if not p.is_undetermined:
            if p.proj_dir:
                paths = [
                        re.sub(r"^{0}".format(re.escape(p.proj_dir)), ".", f.path)
                            for s in p.samples for f in s.files
                        ]
                pdf_paths = [re.sub(r".fastq.gz$", ".qc.pdf", p) for p in paths]
                compute_md5(os.path.join(demultiplex_dir, p.proj_dir), threads, paths + pdf_paths)
            else: # Project files are in root of demultiplexing dir
                paths = [f.path for s in p.samples for f in s.files]
                pdf_paths = [re.sub(r".fastq.gz$", ".qc.pdf", path) for path in paths]
                compute_md5(demultiplex_dir, threads, ["./" + p for p in paths + pdf_paths])

    # Generate internal reports
    generate_internal_html_report(quality_control_dir, samples)
    
    # For email to customers
    for project in projects:
        if project.name != "Undetermined_indices":
            fname = delivery_dir + "/Email_for_" + project.name + ".xls"
            write_sample_info_table(fname, run_id, project)


    # For internal bookkeeping -- This will not be needed when we move to Clarity LIMS
    # exclusively, but we need to figure out how to get all the stats into LIMS for all 
    # sequencer types.
    fname = delivery_dir + "/Table_for_GA_runs_" + run_id + ".xls"
    write_internal_sample_table(fname, run_id, projects)

    # Summary email for NSC staff
    fname = delivery_dir + "/Summary_email_for_NSC_" + run_id + ".xls"
    write_summary_email(fname, run_id, projects, instrument_type=='hiseq')


