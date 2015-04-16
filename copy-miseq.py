# Copy MiSeq run from primary to secondary storage

# Note: excludes BCL files


import os.path
import sys
import argparse
import subprocess
import datetime


from genologics.lims import *
from common import nsc, utilities, copyfiles


def rename_project_dir():
    pass


def main(process_id):
    process = Process(nsc.lims, id=process_id)
    utilities.running(process, 'Copying files')

    ok = copyfiles.copy_files(process, 'miseq')
    
    if ok:
        seq_process = utilities.get_sequencing_process(process)
        run_id = seq_process.udf['Run ID']
        run_dir = os.path.join(nsc.SECONDARY_STORAGE, run_id)

        rename_project_dir()
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

