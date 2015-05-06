# Script to be called directly by EPP (not via slurm) when starting the NextSeq
# demultiplexing step. 
# Sets steering options for the demultiplexing job in UDFs on the demultiplexing 
# process. The options are set based on the input samples. Other options which 
# do not depend on the inputs should be set as defaults in the LIMS or in the 
# NSC configuration file.

import sys, os
from argparse import ArgumentParser
from genologics.lims import *
import logging
from common import nsc, utilities


def get_sample_sheet_data(ddl_proc):
    """Gets the sample sheet from the process where it is generated"""

    outputs = ddl_proc.all_outputs(unique=True)
    for o in outputs:
        if o.output_type == 'ResultFile' and o.name == 'SampleSheet csv':
            if len(o.files) == 1:
                return o.files[0].download()
    return None



# Key determined parameters:
# - In/out directory


def get_paths(process, seq_process):
    try:
        run_id = seq_process.udf['Run ID']
    except:
        return None

    source_path = os.path.join(nsc.PRIMARY_STORAGE, run_id)
    dest_run_path = os.path.join(nsc.SECONDARY_STORAGE, run_id)
    dest_path = os.path.join(dest_run_path, "Data", "Intensities", "BaseCalls")

    return (source_path, dest_path)


def main(process_id, sample_sheet_file):
    process = Process(nsc.lims, id=process_id)
    seq_proc = utilities.get_sequencing_process(process)
    parent_processes = process.parent_processes()
    parent_pids = set(p.uri for p in parent_processes)
    # This script can only handle the case when there is a single clustering process
    if len(parent_pids) == 1:
        ddl_proc = parent_processes[0]
        sample_sheet_data = get_sample_sheet_data(ddl_proc)
        if sample_sheet_data:
            open(sample_sheet_file, "w").write(sample_sheet_data)

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
        logging.warning("Couldn't find the NextSeq sequencing process")
        return 1



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.debug('Starting "setup demultiplexing" script')
    sys.exit(main(sys.argv[1], sys.argv[2]))

