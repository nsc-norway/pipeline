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


# Key determined parameters:
# - In/out directory


def get_paths(process, seq_process):
    try:
        run_id = seq_process.udf['Run ID']
    except:
        return None

    source_path = os.path.join(nsc.PRIMARY_STORAGE, run_id)
    dest_path = os.path.join(nsc.SECONDARY_STORAGE, run_id)

    return (source_path, dest_path)


def main(process_id):
    process = Process(nsc.lims, id=process_id)
    seq_proc = utilities.get_sequencing_process(process)

    if seq_proc:
        paths = get_paths(process, seq_proc)
        if paths:
            logging.debug('Found source and destination paths')
            process.udf[nsc.SOURCE_RUN_DIR_UDF] = paths[0]
            process.udf[nsc.NS_OUTPUT_RUN_DIR_UDF] = paths[1]
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
    sys.exit(main(sys.argv[1]))

