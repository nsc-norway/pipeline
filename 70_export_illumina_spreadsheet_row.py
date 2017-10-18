import sys
import os
import re

from common import nsc, stats, utilities, lane_info, samples, taskmgr
from genologics.lims import *

TASK_NAME = "70. Special_Output"
TASK_DESCRIPTION = """Output a line for the Illumina spreadsheet for each lane."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'lanes']


# Generate tabular data for run summary page

# This script outputs one line for each lane (just one line for mi/nextseq)

def main(task):
    task.running()
    
    run_id = task.run_id
    work_dir = task.work_dir
    instrument = task.instrument

    process = task.process
    if not process:
        task.fail("Can't do this without LIMS")

    if nsc.SITE != "ous":
        # Hack for now, this is only needed on NSC-OUS
        task.success_finish()
        return
    
    try:
        lane_stats = lane_info.get_from_interop(task.work_dir, task.no_lane_splitting)
    except: # lane_info.NotSupportedException:
        expand_lanes = instrument == "nextseq" and not task.no_lane_splitting

        if task.process: # lims mode
            lane_stats = lane_info.get_from_lims(task.process, instrument, expand_lanes)
        else:
            lane_stats = lane_info.get_from_files(work_dir, instrument, expand_lanes)

    projects = task.projects

    qc_dir = os.path.join(task.bc_dir, "QualityControl" + task.suffix)
    if task.instrument in ["hiseq4k", "hiseqx"]:
        stats.add_duplication_results(qc_dir, projects)
    delivery_dir = os.path.join(qc_dir, "Delivery")
    if not os.path.exists(delivery_dir):
        os.mkdir(delivery_dir)

    seq_process = utilities.get_sequencing_process(process)

    step = Step(task.lims, id=seq_process.id)
    clustering_process = process.parent_processes()[0]
    clustering_step = Step(task.lims, id=clustering_process.id)

    # Gather per-run information for the table, data which are the same for all lanes
    year, month, day = re.match(r"(\d\d)(\d\d)(\d\d)_", task.run_id).groups()

    instrument_name_clean = "".join(c for c in seq_process.udf.get('Instrument Name', "UNKNOWN") if c.isalnum())
    date = "{0}.{1}.20{2}".format(day, month, year)
    run_id = task.run_id
    try:
        fc_position = seq_process.udf['Flow Cell Position']
    except KeyError:
        fc_position = seq_process.udf.get('Flowcell Side', '')
    cluster_instrument = clustering_process.udf.get('cBot2 machine', '')
    
    if instrument == "nextseq":
        # High / Mid output chemistry
        run_mode = seq_process.udf.get('Chemistry', '?').replace("NextSeq ", "") + " output"
    else:
        run_mode = seq_process.udf.get('Run Mode', '')
    
    primary_user = u"{0} {1}".format(
            seq_process.technician.first_name,
            seq_process.technician.last_name
            )

    if 'hiseq' in instrument:
        try:
            sbs_lot_1 = next(lot.lot_number for lot in step.reagentlots.reagent_lots if lot.reagent_kit_name.endswith("SBS Reagents 1/2"))
            sbs_lot_2 = next(lot.lot_number for lot in step.reagentlots.reagent_lots if lot.reagent_kit_name.endswith("SBS Reagents 2/2"))
            sbs_lots = ", ".join([sbs_lot_1, sbs_lot_2])

            fc_lot = next(lot.lot_number for lot in step.reagentlots.reagent_lots if lot.reagent_kit_name.endswith("FC"))
        except StopIteration:
            sbs_lots = ""
            fc_lot = ""

        try:
            cluster_kit_lot_1 = next(lot.lot_number for lot in clustering_step.reagentlots.reagent_lots if lot.reagent_kit_name.endswith("Cluster Kit 1/2"))
            cluster_kit_lot_2 = next(lot.lot_number for lot in clustering_step.reagentlots.reagent_lots if lot.reagent_kit_name.endswith("Cluster Kit 2/2"))
            cluster_kit_lots = ", ".join([cluster_kit_lot_1, cluster_kit_lot_2])
        except StopIteration:
            cluster_kit_lots = ""

    elif instrument == 'miseq':
        fc_lot  = next(lot.lot_number for lot in clustering_step.reagentlots.reagent_lots if lot.reagent_kit_name.endswith(" Kit 2/2"))
        rc_lot  = next(lot.lot_number for lot in clustering_step.reagentlots.reagent_lots if lot.reagent_kit_name.endswith(" Kit 1/2"))
    elif instrument == 'nextseq':
        fc_lot = next(lot.lot_number for lot in clustering_step.reagentlots.reagent_lots if lot.reagent_kit_name.endswith(" FC"))
        rc_lot = next(lot.lot_number for lot in clustering_step.reagentlots.reagent_lots if lot.reagent_kit_name.endswith(" RC"))

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

    # Preload lane artifacts
    lims_lanes = seq_process.all_inputs(unique=True, resolve=True)

    process_lanes = task.lanes
    if not process_lanes:
        process_lanes = lane_stats.keys()

    if seq_process.udf.get('Read 2 Cycles', 0) > 0 or seq_process.type_name == "AUTOMATED - Sequence":
        reads = [1,2]
    else:
        reads = [1]

    if instrument.startswith("hiseq"):
        output_file = os.path.join(
                delivery_dir,
                "Illumina_Table_{0}_{1}.txt".format(instrument_name_clean, fc_position)
                )
    else:
        output_file = os.path.join(
                delivery_dir,
                "Illumina_Table_{0}.txt".format(instrument_name_clean)
                )
    with open(output_file, "w") as out:

        def output(data):
            if isinstance(data, unicode):
                out.write(data.encode('utf-8'))
            elif isinstance(data, float):
                out.write(str(data).replace(".", ","))
            else:
                out.write(str(data))
            out.write("\t")


        # Make a table entry for each lane
        for l in sorted(process_lanes):

            lane = lane_stats[l]

            # Get the data objects From LIMS
            if instrument in ['nextseq', 'miseq']:
                lims_lane = lims_lanes[0]
                assert len(lims_lanes) == 1, "Expected 1 input artifact only"
            else:
                lims_lane = next(
                        lims_lane for lims_lane in lims_lanes
                        if lims_lane.location[1] == "{0}:1".format(l)
                        )
            lims_input_pool = next(
                    i['uri'] for i, o in clustering_process.input_output_maps
                    if o['limsid'] == lims_lane.id
                    )


            # Run general metadata
            output(date)
            output(run_id)
            if lims_lane.qc_flag == "FAILED":
                run_status = "Failed"
            else:
                run_status = "Finished"
            output(run_status)
            output(lims_lane.name)

            # Library
            library = lims_lane.samples[0].project.udf.get('loltest', '')
            output(library)

            # Run Mode
            if instrument in ['hiseq', 'nextseq']:
               output(run_mode)
            # Flow cell position
            if instrument in ['hiseq', 'hiseqx']:
                output(fc_position)
            # Cluster instrument
            if instrument in ['hiseq4k', 'hiseqx']:
                output(cluster_instrument)
            # SBS Kit & Cluster kit Lot #
            if instrument in ['hiseq', 'hiseq4k', 'hiseqx']:
                output(sbs_lots)
                output(cluster_kit_lots)
            # Flow cell and reagent kit lot #
            if instrument in ['miseq', 'nextseq']:
                output(fc_lot)
                output(rc_lot)

            if instrument != "hiseq":
                # qPCR
                output("Robot")
                # qPCR pool mM
                molarity = lims_input_pool.udf.get('Molarity')
                if molarity:
                    output(molarity)
                else:
                    output("")
                output(lims_input_pool.udf.get('Average Fragment Size', ''))

            # Duplication
            if instrument in ['hiseq4k', 'hiseqx', 'miseq', 'nextseq']: # sic
                output(lims_lane.udf.get('% Sequencing Duplicates', ''))

            # Loading concentration (pM)
            output(lims_lane.udf.get('Loading Conc. (pM)'))

            # Primary user
            output(primary_user)

            # Various stats from sequencing and demultiplexing
            output(lane.cluster_den_raw / 1000)
            output(lane.pf_ratio * 100.0)
            output(get_avg_reads(lims_lane, '% Phasing R', reads))
            output(get_avg_reads(lims_lane, 'Clusters PF R', reads) / 1.0e6)
            output(get_avg_reads(lims_lane, '% Bases >=Q30 R', reads))
            output(lims_lane.udf['Yield PF (Gb) R1'] + lims_lane.udf.get('Yield PF (Gb) R2', 0))
            output(get_avg_reads(lims_lane, '% Aligned R', reads))
            output(get_avg_reads(lims_lane, '% Error Rate R', reads))

            out.write('\n')

    task.success_finish()

def get_avg_reads(analyte, field, reads):
    return sum(analyte.udf.get(field + str(r)) for r in reads) / len(reads)

if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

