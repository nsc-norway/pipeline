# Write stats to LIMS
import os
import sys
import requests
from genologics.lims import *
from common import nsc, taskmgr, stats, utilities

TASK_NAME = "50. LIMS stats"
TASK_DESCRIPTION = """Post demultiplexing stats to LIMS (doesn't make an effort
                    to handle the non-LIMS case, obviously)."""
TASK_ARGS = ['work_dir', 'lanes']


udf_list = [
        '# Reads', '# Reads PF', 'Yield PF (Gb)', '% of Raw Clusters Per Lane',
        '% of PF Clusters Per Lane',
        '% Perfect Index Read', 'One Mismatch Reads (Index)',
        '% Bases >=Q30', 'Ave Q Score', '%PF'
        ]

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
    run_stats = stats.get_stats(
            utilities.get_instrument_by_runid(task.run_id),
            task.work_dir,
            aggregate_lanes = task.no_lane_splitting,
            aggregate_reads = True,
            miseq_uniproject=next(p.name for p in projects if not p.is_undetermined),
            suffix=task.suffix
            )

    post_stats(task.process, projects, run_stats)

    task.success_finish()


def post_stats(process, projects, demultiplex_stats):
    """Find the resultfiles in the LIMS and post the demultiplexing
    stats.
    """ 
    projects_map = {}
    for project in projects:
        samples_map = {}
        for sample in project.samples:
            samples_map[sample.name] = sample

        projects_map[project.name] = samples_map


    update_artifacts = []
    for coordinates, stats in demultiplex_stats.items():
        # Note: while it may seem that this works for both aggregate_reads and
        # separate reads, it does not, the code must be changed for separate reads
        lane, project, sample_name = coordinates[0:3]
        
        if sample_name: # Not undetermined
            try:
                limsid = projects_map[project][sample_name].sample_id
            except KeyError:
                continue # Skip unknown samples / project
            resultfile = get_resultfile(process, lane, limsid, 1)
            if resultfile:
                for statname in udf_list:
                    try:
                        resultfile.udf[statname] = stats[statname]
                    except KeyError:
                        pass
                update_artifacts.append(resultfile)
            

        else: # Undetermined: sample_name = None in demultiplex_stats
            lane_analyte = get_lane(process, lane)
            if lane_analyte:
                lane_analyte.udf[nsc.LANE_UNDETERMINED_UDF] = stats['% of PF Clusters Per Lane']
                update_artifacts.append(lane_analyte)

    nsc.lims.put_batch(update_artifacts)


def get_resultfile(process, lane, input_limsid, read):
    """Find a result file artifact which is an output of process and
    which corresponds to input_limsid. This is used to find the output of
    the demultiplexing process.

    input_limsid is any derived sample or a submitted sample.
    
    The correct output artifact is identified by two criteria: 
     - The output must come from an input which matches the lane number. I.e. the input
       must be in the correct position in the flowcell in the LIMS.
     - The name of the output must match a pattern, potentially including the 
       following:
        * The name of the submitted sample in the LIMS
        * The read number (1 or 2 for paired end read)
       The name of the submitted sample for comparison is retrieved by querying the
       LIMS for the input_limsid, and using the sample which is associated with this
       LIMS ID.

    Returns the associated result file, If the file can't be found, in general an 
    error is thrown (relying on underlying functions to throw, like HTTPError).
    """

    try:
        input_sample = Artifact(nsc.lims, id=input_limsid).samples[0]
    except requests.exceptions.HTTPError as e:
        # If the sample is not pooled, we may get the Sample LIMSID in the 
        # sample sheet, not the Analyte LIMSID. So we request the sample 
        # with this ID.
        # Would only do this for 404, but there is no e.response.status_code
        # (that is, e.response is None)
        input_sample = Sample(nsc.lims, id=input_limsid)
        try:
            input_sample.get()
        except requests.exceptions.HTTPError:
            return None

    # Find the result file corresponding to this artifact
    for i, o in process.input_output_maps:
        input = i['uri']
        if input.location[1] in ['{0}:1'.format(lane), 'A:1']: # Use A:1 for NextSeq, MiSeq
            if o['output-type'] == "ResultFile" and o['output-generation-type'] == "PerReagentLabel":
                output = o['uri']
                # The constant FASTQ_OUTPUT corresponds to the name configured in
                # the LIMS process type for the ResultFiles representing the "measurements", 
                # i.e. the demultiplexing stats. It will be something like
                # "{SubmittedSampleName}" to indicate that each sample will be named 
                # after the corresponding submitted sample.
                if output.name == nsc.FASTQ_OUTPUT.format(
                        lane=lane,
                        sample_name=input_sample.name.encode('utf-8'),
                        read=read
                        ):
                    return output


def get_lane(process, lane):
    """Get the input corresponding to a given lane ID. 
    
    Returns None if no such lane."""
    for input in process.all_inputs():
        if (lane == 1 or lane == "X") and input.location[1] == 'A:1': # Use A:1 for NextSeq, MiSeq
            return input
        elif input.location[1] == '%d:1' % lane:
            return input


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)
