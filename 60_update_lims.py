# Write stats to LIMS
import os
import sys
import requests
from collections import defaultdict
from genologics.lims import *
from common import nsc, taskmgr, stats, utilities, lane_info

TASK_NAME = "60. LIMS stats"
TASK_DESCRIPTION = """Post demultiplexing stats to LIMS (doesn't make an effort
                    to handle the non-LIMS case, obviously)."""
TASK_ARGS = ['work_dir', 'lanes']


udf_list = [
        '# Reads', '# Reads PF', 'Yield PF (Gb)', '% of Raw Clusters Per Lane',
        '% of PF Clusters Per Lane',
        '% Perfect Index Read', '% One Mismatch Reads (Index)',
        '% Bases >=Q30', 'Ave Q Score', '%PF'
        ]
# Note: see main(), adding stats for Hi4k/X

def main(task):
    task.running()

    if not task.process:
        task.fail("Can't use this script without LIMS (--pid), sorry.")

    # We need the projects list to match the sample name and project name
    # to the corresponding LIMS-ID in the sample sheet. The sample name
    # and project combo is unique, but may be difficult to look up in the 
    # LIMS if the name contains any funny characters.
    projects = task.projects

    # Aggregate lanes must match the setting used for demultiplexing
    # Aggregate reads must match the setup in the LIMS -- it should be 
    # true if there is one resultfile for both reads, false if separate
    # resultfiles.
    #print "Getting stats"
    run_stats = stats.get_stats(
            task.instrument,
            task.work_dir,
            aggregate_lanes = task.no_lane_splitting,
            aggregate_reads = True,
            suffix=task.suffix
            )
    # Get lane stats and lane metrics
    # Here, lane stats refers to the data from lane_info module, containing primarily
    # the well occupancy percentage. Lane metrics is based on superDUPr and contains the
    # duplication rate.
    try:
        lane_stats = lane_info.get_from_interop(task.work_dir, task.no_lane_splitting)
    except lane_info.NotSupportedException:
        lane_stats = {}
    qc_dir = os.path.join(task.bc_dir, "QualityControl" + task.suffix)
    if task.instrument in ['hiseq4k', 'hiseqx', 'novaseq']:
        stats.add_duplication_results(qc_dir, projects)
        lane_metrics = get_lane_metrics(projects)
    else:
        lane_metrics = {}
    post_stats(task.lims, task.process, projects, run_stats, lane_metrics, lane_stats)
    task.success_finish()


def post_stats(lims, process, projects, demultiplex_stats, lane_metrics, lane_stats):
    """Find the resultfiles in the LIMS and post the demultiplexing
    stats.
    """ 
    #print "Loading samples"
    lims.get_batch(process.all_inputs(unique=True) + process.all_outputs(unique=True))
    lims.get_batch(sum((a.samples for a in process.all_inputs()), []))
    #print "Done loading"

    update_artifacts = set()
    for coordinates, stats in list(demultiplex_stats.items()):
        # Note: while it may seem that this works for both aggregate_reads and
        # separate reads, it does not, the code must be changed for separate reads
        lane, sample_id = coordinates[0:2]
        
        if sample_id: # Not undetermined
            limsid = None
            for tproject in projects:
                for sample in tproject.samples:
                    if sample.sample_id == sample_id:
                        if any(f.lane == lane for f in sample.files):
                            limsid = sample.limsid
                            sample_index = sample.sample_index
            
            if limsid is None:
                continue # Skip unknown samples / project
            resultfile = get_resultfile(lims, process, lane, limsid, 1)
            if resultfile:
                for statname in udf_list:
                    try:
                        resultfile.udf[statname] = stats[statname]
                    except KeyError:
                        pass
                resultfile.udf['Sample sheet position'] = sample_index
                update_artifacts.add(resultfile)


        else: # Undetermined: sample_name = None in demultiplex_stats
            lane_analyte = get_lane(process, lane)
            if lane_analyte:
                lane_analyte.udf[nsc.LANE_UNDETERMINED_UDF] = stats['% of PF Clusters Per Lane']
                update_artifacts.add(lane_analyte)

    for lane, metric in list(lane_metrics.items()):
        lane_analyte = get_lane(process, lane)
        if lane_analyte:
            duplicates = metric.get('% Sequencing Duplicates', None)
            if duplicates is not None:
                lane_analyte.udf['% Sequencing Duplicates'] = duplicates
                update_artifacts.add(lane_analyte)
            stats = lane_stats.get(lane)
            if stats and stats.occupancy: # Attempt to get occupancy, None if not supported
                lane_analyte.udf['% Occupied Wells'] = stats.occupancy
                update_artifacts.add(lane_analyte)
    #print "Updating"
    lims.put_batch(update_artifacts)


def get_lane_metrics(projects):
    lane_n_with_dup = defaultdict(int)
    lane_count = defaultdict(int)
    for project in projects:
        for sample in project.samples:
            for f in sample.files:
                if f.i_read == 1 and f.stats and 'fastdup reads with duplicate' in f.stats:
                    lane_n_with_dup[f.lane] += f.stats['fastdup reads with duplicate']
                    lane_count[f.lane] += f.stats['fastdup reads analysed']
    metrics = {}
    for lane in list(lane_n_with_dup.keys()):
        metrics[lane] = {'% Sequencing Duplicates': lane_n_with_dup[lane] * 100.0 / lane_count[lane]}
    return metrics


def get_resultfile(lims, process, lane, input_limsid, read):
    """Find a result file artifact which is an output of process and
    which corresponds to input_limsid. This is used to find the output of
    the demultiplexing process.

    input_limsid is any derived sample or a submitted sample.
    
    The correct output artifact is identified by two criteria: 
     - The output must come from an input which matches the lane number. I.e. the input
       must be in the correct position in the flowcell in the LIMS.
     - The output is assumed to have just one associated Sample in LIMS, and that
       sample's ID must match the provided LIMS-ID. The argument input_limsid may
       alternatively 

    Returns the associated "result file" artifact or None if it cannot be found.
    """

    try:
        input_sample = Artifact(lims, id=input_limsid).samples[0]
    except requests.exceptions.HTTPError as e:
        # If the sample is not pooled, we may get the Sample LIMSID in the 
        # sample sheet, not the Artifact LIMSID. So if the input_limsid doesn't
        # work as a Artifact, we request the sample with this ID (and failing that,
        # give up).
        input_sample = Sample(lims, id=input_limsid)
        try:
            input_sample.get()
        except requests.exceptions.HTTPError:
            return None

    # Find the result file corresponding to this artifact
    for i, o in process.input_output_maps:
        input = i['uri']
        if input.location[1] in ['{0}:1'.format(lane), 'A:1'] or input.location[0].type_name == "Library Tube":
                            # Use A:1 for NextSeq, MiSeq; Library Tube for NovaSeq Standard workflow
            if o['output-type'] == "ResultFile" and o['output-generation-type'] == "PerReagentLabel":
                output = o['uri']
                # Match the output based on LIMS-ID.
                if output.samples[0].id == input_sample.id:
                    return output
                    

def get_lane(process, lane):
    """Get the input corresponding to a given lane ID. 
    
    Returns None if no such lane."""
    for input in process.all_inputs():
        if lane == 1 and input.location[1] == 'A:1': # Use A:1 for NextSeq, MiSeq
            return input
        elif lane == "X":
            return input
        elif input.location[1] == '{}:1'.format(lane):
            return input


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

