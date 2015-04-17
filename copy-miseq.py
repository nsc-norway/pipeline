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

def rename_project_dirs():
    proj_name = sample_sheet['header']['Experiment Name']
    for i, sam in enumerate(sample_sheet['data']):
        sample_name = sam['Sample_Name']


def main(process_id):
    process = Process(nsc.lims, id=process_id)
    utilities.running(process, 'Copying files')

    ok = copyfiles.copy_files(process, 'miseq')
    
    if ok:
        seq_process = utilities.get_sequencing_process(process)
        run_id = seq_process.udf['Run ID']
        run_dir = os.path.join(nsc.SECONDARY_STORAGE, run_id)
        basecalls_dir = os.path.join(run_dir, "Data", "Intensities", "BaseCalls")
        sample_sheet = get_sample_sheet(run_dir)
        if sample_sheet:
            reads = [1]
            try:
                if seq_process.udf['Read 2 Cycles']:
                    reads.append(2)
            except KeyError:
                pass
            demultiplex.create_projdir_ne_mi(run_id, run_dir, sample_sheet, 1, reads)

        utilities.success_finish(process)
    else:
        utilities.fail(process, 'rsync returned an error')
    return ok
    


if __name__ == '__main__':
    try:
        ok = main(sys.argv[1])
    except:
        process = Process(nsc.lims, id = sys.argv[1])
        utilities.fail(process, "Unexpected: " + str(sys.exc_info()[1]))
        raise
    sys.exit(0 if ok else 1)

