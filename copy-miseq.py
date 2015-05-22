# Copy MiSeq run from primary to secondary storage

# Note: excludes BCL files


import os.path
import sys
import argparse
import subprocess
import datetime


from genologics.lims import *
from common import (
        nsc,
        utilities,
        copyfiles,
        parse,
        demultiplex
        )


def get_sample_sheet(run_dir):
    try:
        data = open(os.path.join(run_dir, "SampleSheet.csv")).read()
        return parse.parse_ne_mi_seq_sample_sheet(data)
    except IOError:
        return None


def main(process_id):
    process = Process(nsc.lims, id=process_id)
    utilities.running(process, 'Copying files')

    seq_process = utilities.get_sequencing_process(process)
    run_id = seq_process.udf['Run ID']
    ok = copyfiles.copy_files(run_id, 'miseq')
    
    if ok:
        run_dir = os.path.join(nsc.SECONDARY_STORAGE, run_id)
	process.udf[nsc.COPY_MISEQ_DEST_UDF] = run_dir
        sample_sheet = get_sample_sheet(run_dir)
        if sample_sheet:
            reads = [1]
            try:
                if seq_process.udf['Read 2 Cycles']:
                    reads.append(2)
            except KeyError:
                pass
            basecalls_dir = os.path.join(output_dir, "Data", "Intensities", "BaseCalls") 
            demultiplex.create_projdir_ne_mi(run_id, basecalls_dir, sample_sheet, 1, reads)

        utilities.success_finish(process)
    else:
        utilities.fail(process, 'rsync returned an error')
    return ok
    


if __name__ == '__main__':
    with utilities.error_reporter():
        ok = main(sys.argv[1])
        sys.exit(0 if ok else 1)

