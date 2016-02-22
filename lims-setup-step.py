#!/bin/env python

# Script to be called when entering the Demultiplexing and QC step. 

# Sets the source and destination directories based on information in the LIMS. 
# Copies the sample sheet from the associated cluster generation process to the 
# current LIMS process. The options are set based on the input samples. Other 
# options which do not depend on the inputs should be set as defaults in the LIMS
# or in the NSC configuration file.

# Use:
# python lims-setup-step.py <PROCESS_ID> <SAMPLE_SHEET_LIMSID>

# Should be configured with the LIMSID of the "Input sample sheet" file as the second
# argument, so when it writes this file, it will be uploaded to LIMS automatically.

# In general it will not exit with an error condition, as it should be possible 
# to supply this info manually if it can't be found automatically.


import sys
import os
from genologics.lims import *
import logging
from common import nsc, utilities

# Set these options
if nsc.SITE == "cees":
    CHECKED = {
            "hiseq": [
                "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC", "Close when finished"
                ]
            }
else:
    CHECKED = {
            "hiseq": [
                "Auto 10. Copy run", "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC", "Auto 90. Prepare delivery",
                "Close when finished"
                ],
            "miseq": [
                "Auto 10. Copy run", "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC", "Auto 90. Prepare delivery",
                "Close when finished"
                ],
            "nextseq": [
                "No lane splitting",
                "Auto 10. Copy run", "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC",
                "Auto 85. Copy run again (NextSeq)", "Auto 90. Prepare delivery",
                "Close when finished"
                ],
            "hiseqx": [
                "Auto 10. Copy run", "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC", "Auto 90. Prepare delivery",
                "Close when finished"
                ],
            }


def get_sample_sheet_data(cluster_proc, fcid):
    """Gets the sample sheet from the clustering- or "...load samples" process"""

    outputs = cluster_proc.all_outputs(unique=True)
    for io in cluster_proc.input_output_maps:
        i = io[0]
        o = io[1]
        if o['output-type'] == 'ResultFile' and o['output-generation-type'] == 'PerAllInputs':
            if o['uri'].name == 'SampleSheet csv':
                if len(o['uri'].files) == 1:
                    f = o['uri'].files[0]
                    if f.original_location == "{0}.csv".format(fcid):
                        return f.download()
    return None


def main(process_id, sample_sheet_file):
    process = Process(nsc.lims, id=process_id)

    fcids = set()
    wells = set()
    for i in process.all_inputs():
        fcids.add(i.location[0].name)
        wells.add(i.location[1])

    if len(fcids) != 1:
        logging.error("Multiple flowcells in inputs, this is going to end in tears")
        return 1

    fcid = next(iter(fcids))

    if len(wells) != i.location[0].occupied_wells:
        # Use a subset of the lanes
        lanes = "".join(str(l.split(":")[0]) for l in sorted(wells))
        process.udf[nsc.LANES_UDF] = lanes

    seq_proc = utilities.get_sequencing_process(process)
    parent_processes = process.parent_processes()
    parent_pids = set(p.uri for p in parent_processes)

    # This script can only handle the case when there is a single clustering process
    # (or for Mi/NextSeq, ...Load samples process)
    if len(parent_pids) == 1:
        # look for clustering (HiSeq) or "load samples" proceess (Ne/MiSeq)
        # This is where the sample sheet is generated
        parent_proc = parent_processes[0]
        sample_sheet_data = get_sample_sheet_data(parent_proc, fcid)
        if sample_sheet_data:
            open(sample_sheet_file, "w").write(sample_sheet_data)
    else:
        logging.info("Cannot auto-detect sample sheet when there are more than one clustering processes")

    if seq_proc:
        try:
            run_id = seq_proc.udf['Run ID']
        except KeyError:
            run_id = None

        if run_id:
            process.udf[nsc.RUN_ID_UDF] = run_id

            logging.debug('Found source and destination paths')
            source_path = os.path.join(nsc.PRIMARY_STORAGE, run_id)
            dest_path = os.path.join(nsc.SECONDARY_STORAGE, run_id)
            process.udf[nsc.SOURCE_RUN_DIR_UDF] = source_path
            process.udf[nsc.WORK_RUN_DIR_UDF] = dest_path
        else:
            logging.debug('Unable to determine source and destination paths')

        instrument = utilities.get_instrument(seq_proc)
        for udf in CHECKED.get(instrument, []):
            process.udf[udf] = True

        logging.debug('Saved settings in the process')

    else:
        logging.warning("Couldn't find the sequencing process")
    process.put()

    logging.info("Program completed")



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.debug('Starting "setup demultiplexing" script')
    sys.exit(main(sys.argv[1], sys.argv[2]))

