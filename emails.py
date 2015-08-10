import sys, os

from genologics.lims import *
from common import nsc, stats, utilities, lane_info, sample

# Generate reports for email based on demultiplexing stats

def make_reports(work_dir, run_id, project_stats, lane_stats):
    qc_dir = os.path.join(work_dir, "Data", "Intensities", "BaseCalls", "QualityControl")
    delivery_dir = os.path.join(qc_dir, "Delivery")
    for d in [qc_dir, delivery_dir]:
        try:
            os.mkdir(d)
        except OSError:
            pass # Assume it exists

    projects = sample.get_projects(run_id

    for project in projects:
        if project.name != "Undetermined_indices":
            fname = delivery_dir + "/Email_for_" + project.name + ".xls"
            write_sample_info_table(fname, run_id, project)

    fname = delivery_dir + "/Table_for_GA_runs_" + run_id + ".xls"
    write_internal_sample_table(fname, run_id, projects)

    fname = delivery_dir + "/Summary_email_for_NSC_" + run_id + ".xls"
    write_summary_email(fname, run_id, projects, instrument_type=='hiseq')


def main_lims(process_id):
    process = Process(nsc.lims, id=process_id)
    run_id = process.udf[nsc.RUN_ID_UDF]
    instument = utilities.get_instrument_from_runid(run_id)
    work_dir = utilities.get_udf(
            process, nsc.WORK_RUN_DIR_UDF,
            os.path.join(nsc.SECONDARY_STORAGE, run_id)
            )
    
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
    
    projects = sample.get_projects(sample_sheet)

    lane_stats = lane_info.get_from_lims(process, instrument)

    make_reports(work_dir, run_id, project_stats, lane_stats)


def write_sample_info_table(output_path, runid, project):
    with open(output_path, 'w') as out:
        out.write('--------------------------------		\n')
        out.write('Email for ' + project.name + "\n")
        out.write('--------------------------------		\n\n')
        nsamples = len(project.samples)
        out.write('Sequence ready for download - sequencing run ' + runid + ' - Project_' + project.name + ' (' + str(nsamples) + ' samples)\n\n')

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
                        if f.stats.has_key('% of Raw Clusters Per Lane'):
                            out.write("%4.2f" % (f.stats['% of Raw Clusters Per Lane']) + "%\t")
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
            if undetermined_file and not undetermined_file.empty:
                if undetermined_file.stats.has_key('% of Raw Clusters Per Lane'):
                    out.write("%4.2f" % (undetermined_file.stats['% of Raw Clusters Per Lane'],) + "%\t")
                elif undetermined_file.stats.has_key('% of PF Clusters Per Lane'):
                    print "Warning: No info about % raw clusters per lane, using PF clusters."
                    out.write("%4.2f" % (undetermined_file.stats['% of PF Clusters Per Lane'],) + "%\t")
                else:
                    out.write("-\t")
            else:
                out.write("-\t")
            out.write("ok\n")


if __name__ == "__main__":
    main_lims(sys.argv[1])

