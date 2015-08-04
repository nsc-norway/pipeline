# Write stats to LIMS
from common import nsc, stats, utilities
from genologics.lims import *

def main(process_id):
    process = Process(nsc.lims, id=process_id)
    utilities.running(process, nsc.CJU_SAVING_STATS)

    run_id = process.udf[nsc.RUN_ID_UDF]
    demultiplex_dir = utilities.get_udf(
            process, nsc.WORK_RUN_DIR_UDF,
            os.path.join(nsc.SECONDARY_STORAGE, run_id)
            )

    run_stats = stats.get_bcl2fastq_stats(
            os.path.join(demultiplex_dir, "Stats"),
            aggregate_lanes = True,
            aggregate_reads = True
            )

    post_stats(process, run_stats)


def post_stats(process, demultiplex_stats):
    for coordinates, stats in demultiplex_stats.items():
        lane, limsid = coordinates[0:2]

        # The LIMS ID in the sample sheet (and then in the stats) will be the ID of the 
        # derived sample that went into a pool that was sequenced, or one that went 
        # directly on the sequencer.

                            
        if limsid:
            resultfile = get_resultfile(process, lane, limsid, 1)
            for statname in udf_list:
                try:
                    lims_fastqfile.udf[statname] = stats[statname]
                except KeyError:
                    pass
            lims_fastqfile.put()
            

        else: # Undetermined: limsid = None in demultiplex_stats
            lane_analyte = get_lane(lane)
            if lane_analyte:
                lane_analyte.udf[nsc.LANE_UNDETERMINED_UDF] = stats['% of PF Clusters Per Lane']
                lane_analyte.put()


def get_resultfile(process, lane, input_limsid, read):
    """Find a result file artifact which is an output of process and
    which corresponds to input_limsid.

    input_limsid is any derived sample which is not a pool, or a submitted sample.
    
    The correct output is identified by two criteria: 
     - The output must come from an input which matches the lane number. The input
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

    lane, input_limsid, read = coordinates
    try:
        input_sample = Artifact(nsc.lims, id=input_limsid).samples[0]
    except requests.exceptions.HTTPError as e:
        # If the sample is not pooled, we may get the Sample LIMSID in the 
        # sample sheet, not the Analyte LIMSID. So we request the sample 
        # with this ID.
        # Would only do this for 404, but there is no e.response.status_code
        # (that is, e.response is None)
        input_sample = Sample(nsc.lims, id=input_limsid)
        input_sample.get()

    # Find the result file corresponding to this artifact
    for i, o in process.input_output_maps:
        input = i['uri']
        if input.location[1] in ['%d:1' % lane, 'A:1']: # Use A:1 for NextSeq, MiSeq
            if o['output-type'] == "ResultFile" and o['output-generation-type'] == "PerReagentLabel":
                output = o['uri']
                # The LIMSID 
                if output.name == nsc.FASTQ_OUTPUT.format(
                        lane=lane,
                        sample_name=input_sample.name,
                        read=read
                        ):
                    return output


def get_lane(process, lane):
    """Get the input corresponding to a given lane ID. 
    
    Returns None if no such lane."""
    for input in process.all_inputs():
        if lane == 1 and input.location[1] == 'A:1': # Use A:1 for NextSeq, MiSeq
            return input
        if i.location[1] == '%d:1' % lane:
            return input



if __name__ == "__main__":
    main(sys.argv[1])

