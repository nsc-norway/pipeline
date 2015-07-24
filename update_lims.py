# Write stats to LIMS
from common import nsc, stats, utilities
from genologics.lims import *

def main(process_id):
    process = Process(nsc.lims, id=process_id)
    utilities.running(process, nsc.CJU_SAVING_STATS)

    seq_process = utilities.get_sequencing_process(process)
    
    run_id = seq_process.udf['Run ID']
    demultiplex_dir = utilities.get_udf(
            process, nsc.WORK_RUN_DIR_UDF,
            os.path.join(nsc.SECONDARY_STORAGE, run_id)
            )

    run_stats = stats.get_bcl2fastq_stats(
            os.path.join(demultiplex_dir, "Stats"),
            aggregate_lanes = True,
            aggregate_reads = True
            )

    update_lims(process, run_stats)

    



def update_lims(process, stats):
    for key, value in stats.items(}:
        lane, sample_id, read = coordinates



def make_id_resultfile_map(process, sample_sheet_data, reads):
    """Produces map from lane, sample-ID and read to output 
    analyte. Lane is always 1 for NS, but keeping it in for consistency."""
    themap = {}
    for entry in sample_sheet_data:
        name = entry['samplename']
        input_limsid = entry['sampleid']
        try:
            input_sample = Artifact(nsc.lims, id=input_limsid).samples[0]
        except requests.exceptions.HTTPError as e:
            # If the sample is not pooled, we'll get the Sample LIMSID in the 
            # sample sheet, not the Analyte LIMSID. So we request the sample 
            # with this ID.
            # Would only do this for 404, but there is no e.response.status_code
            # (that is, e.response is None)
            input_sample = Sample(nsc.lims, id=input_limsid)
            input_sample.get()

        for output in process.all_outputs(unique=True):
            #for read in reads:
            #    for lane in xrange(1, 5):
            if output.name == nsc.NEXTSEQ_FASTQ_OUTPUT.format(
                    input_sample.name
                    ):
                themap[("X",input_limsid,1)] = output
    return themap


def populate_results(process, ids_resultfile_map, demultiplex_stats):
    """Set UDFs on inputs (analytes representing the lanes) and output
    files (each fastq file).
    """
    inputs = dict((i.location[1], i) for i in process.all_inputs(unique=True))
    if len(set(i.location[0] for i in inputs.values())) != 1:
        print "error: Wrong number of flowcells detected"
        return False

    for coordinates, stats in demultiplex_stats.items():
        if len(coordinates) > 1:
            lane = coordinates[0]
            sample_name = coordinates[1]
        else:
            lane = "X"
            sample_name = coordinates[0]

        lims_fastqfile = None
        try:
            lims_fastqfile = ids_resultfile_map[coordinates]
            undetermined = False
        except KeyError:
            if sample_name:
                undetermined = re.match(r"lane\d$", sample_name)
            else:
                undetermined = True

        if lims_fastqfile:
            for statname in udf_list:
                try:
                    lims_fastqfile.udf[statname] = stats[statname]
                except KeyError:
                    pass
            lims_fastqfile.put()
    
        elif undetermined:
            try:
                analyte = inputs['{0}:1'.format(lane)]
            except KeyError:
                if len(inputs) == 1:
                    # NextSeq: flow cell has position A:1
                    analyte = inputs['A:1']
                else:
                    raise
            analyte.udf[nsc.LANE_UNDETERMINED_UDF] = stats['% of PF Clusters Per Lane']
            analyte.put()

    return True


def post_stats(demultiplex_stats, input_output_maps):

    for coordinates, stats in demultiplex_stats.items():
        limsid = coordinates[1]
        try:
            input_sample = Artifact(nsc.lims, id=input_limsid).samples[0]
        except requests.exceptions.HTTPError as e:
            # If the sample is not pooled, we'll get the Sample LIMSID in the 
            # sample sheet, not the Analyte LIMSID. So we request the sample 
            # with this ID.
            # Would only do this for 404, but there is no e.response.status_code
            # (that is, e.response is None)
            input_sample = Sample(nsc.lims, id=input_limsid)
            input_sample.get()

        for i, o in input_output_maps:
            if i['uri'].name == "TODO":
                pass






if __name__ == "__main__":
    main(sys.argv[1])
