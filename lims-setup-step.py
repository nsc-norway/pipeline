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
                    if f.original_location == "{0}.csv".format(fcid)
                        return f.download()
    return None


def get_paths(process, seq_process):
    try:
        run_id = seq_process.udf['Run ID']
    except:
        return None

    source_path = os.path.join(nsc.PRIMARY_STORAGE, run_id)
    dest_path = os.path.join(nsc.SECONDARY_STORAGE, run_id)

    return (source_path, dest_path)


def main(process_id, sample_sheet_file):
    process = Process(nsc.lims, id=process_id)

    fcids = set()
    for i in process.all_inputs():
        fcids.add(i.location[0].name)

    if len(fcids) != 1:
        logging.error("Multiple flowcells in inputs, this is going to end in tears")
        return 1

    fcid = next(iter(fcids))

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
        paths = get_paths(process, seq_proc)
        if paths:
            logging.debug('Found source and destination paths')
            process.udf[nsc.SOURCE_RUN_DIR_UDF] = paths[0]
            process.udf[nsc.DEST_FASTQ_DIR_UDF] = paths[1]
        else:
            logging.debug('Unable to determine source and destination paths')

        process.put()
        logging.debug('Saved settings in the process')

    else:
        logging.warning("Couldn't find the sequencing process")
        return 1

    logging.info("Program completed")



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.debug('Starting "setup demultiplexing" script')
    sys.exit(main(sys.argv[1], sys.argv[2]))

