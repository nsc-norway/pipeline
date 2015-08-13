import sys, os

from genologics.lims import *
from common import nsc, stats, utilities, lane_info, samples

# Generate reports after FastQC has completed

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


#def 


if __name__ == "__main__":
    main_lims(sys.argv[1])

