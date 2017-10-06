import sys
import os
import re

from common import nsc, stats, utilities, lane_info, samples, taskmgr

TASK_NAME = "60. Special_Output"
TASK_DESCRIPTION = """Output a line for the Illumina spreadsheet for each lane."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'lanes']


# Generate reports for emails based on demultiplexing stats


def main(task):
    task.running()
    
    run_id = task.run_id
    work_dir = task.work_dir
    instrument = utilities.get_instrument_by_runid(run_id)

    process = task.process
    if not process:
        task.fail("Can't do this without LIMS")
    
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
            aggregate_reads = True,
            suffix=task.suffix
            )
    samples.add_stats(projects, run_stats)

    #samples.flag_empty_files(projects, work_dir)
    qc_dir = os.path.join(task.bc_dir, "QualityControl" + task.suffix)
    if task.instrument in ["hiseq4k", "hiseqx"]:
        stats.add_duplication_results(qc_dir, projects)
    delivery_dir = os.path.join(qc_dir, "Delivery")
    if not os.path.exists(delivery_dir):
        os.mkdir(delivery_dir)


    clustering_process = process.parent_processes()[0]

    run_data = {}

    # Gather per-run information for the table, data which are the same for all lanes
    year, month, day = re.match(r"(\d\d)(\d\d)(\d\d)_", task.run_id).groups()

    date = "{0:02}.{0:02}.20{0:02}".format(day, month, year)
    run_id = task.run_id
    run_status = "Finished"
    comments = ""
    library = ""
    fc_position = process.get('Flow Cell Position')
    cluster_instrument = clustering_process.get('cBot2 machine')
    run_mode = process.get('Run Mode') # TODO
    sbs_lot = ""
    flow_cell_lot_no = ""

    



    # This code was copied from the summary email generator. In general, it's a bit ugly since it needs to 
    # add back together the stats, which are stratified by file in the object tree.

    # Dict: lane ID => (lane object, project object)
    # If there are multiple projects on a lane, one of them will be used.
    lane_proj = dict((f.lane, proj) for proj in projects
            for s in proj.samples for f in s.files if not proj.is_undetermined)

    # lane ID => file object for undetermined reads.
    lane_undetermined = dict(
            (f.lane, f)
            for proj in projects
            for s in proj.samples
            for f in s.files
            if proj.is_undetermined)

    with open("Illumina_Table.txt", "w") as output_file:

        # Make a table entry for each lane
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

    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

