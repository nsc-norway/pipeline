import sys, os

from genologics.lims import *
from common import nsc, stats, utilities, lane_info, samples

# Generate reports for emails based on demultiplexing stats


# This has a dependency on LIMS because it preferentially gets the 

def make_reports(work_dir, run_id, projects, lane_stats):
    basecalls_dir = os.path.join(work_dir, "Data", "Intensities", "BaseCalls")
    delivery_dir = os.path.join(basecalls_dir, "Delivery")
    os.umask(007)
    try:
        os.mkdir(delivery_dir)
    except OSError:
        pass # Assume it exists

    for project in projects:
        if not project.is_undetermined:
            fname = delivery_dir + "/Email_for_" + project.name + ".xls"
            write_sample_info_table(fname, run_id, project)

    fname = delivery_dir + "/Table_for_GA_runs_" + run_id + ".xls"
    write_internal_sample_table(fname, run_id, projects, lane_stats)

    fname = delivery_dir + "/Summary_email_for_NSC_" + run_id + ".xls"
    instrument_type = utilities.get_instrument_by_runid(run_id)
    write_summary_email(fname, run_id, projects, instrument_type=='hiseq', lane_stats)


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
    samples.flag_empty_files(projects, work_dir)

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
    samples.flag_empty_files(projects, work_dir)

    make_reports(work_dir, run_id, projects, lane_stats)


def write_sample_info_table(output_path, runid, project):
    with open(output_path, 'w') as out:
        out.write('--------------------------------		\n')
        out.write('Email for ' + project.name + "\n")
        out.write('--------------------------------		\n\n')
        nsamples = len(project.samples)
        out.write('Sequence ready for download - sequencing run ' + runid + ' - Project_' + project.name + ' (' + str(nsamples) + ' samples)\n\n')

        files = sorted(
                ((s,fi) for s in project.samples for fi in s.files),
                key=lambda (s,f): (f.lane, s.name, f.read_num)
                )
        for s,f in files:
            out.write(os.path.basename(f.path) + "\t")
            out.write(utilities.display_int(f.stats['# Reads PF']) + "\t")
            out.write("fragments\n")



def write_internal_sample_table(output_path, runid, projects, lane_stats):
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
                if not proj.is_undetermined),
                key=operator.attrgetter('name')
                )
        for s in samples:
            for f in s.files:
                if f.read_num == 1:
                    out.write(s.name + "\t")
                    lane = lane_stats[f.lane]
                    out.write(utilities.display_int(lane[0]) + "\t")
                    out.write(utilities.display_int(lane[1]) + "\t")
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



def write_summary_email(output_path, runid, projects, print_lane_number, lane_stats):
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
        lane_proj = dict((f.lane, proj) for proj in projects
                for s in proj.samples for f in s.files if not proj.is_undetermined)
        # lane ID => file object (will be the last one)
        lane_undetermined = dict((f.lane, f) for proj in projects
                for s in proj.samples for f in s.files if not proj.is_undetermined)

        for l in sorted(lane_proj.keys()):
            proj = lane_proj[l]
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
                    if f.lane == l and f.read_num == 1 and not f.empty)
            out.write(utilities.display_int(cluster_no) + '\t')
            lane = lane_stats[l]
            out.write("%4.2f" % (lane[2]) + "\t")
            out.write(utilities.display_int(lane[0]) + '\t')
            out.write(utilities.display_int(lane[1]) + '\t')
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
