# Copy global run stats/metadata files from primary to secondary storage

# This script copies data from the storage are written to by the sequencers,
# to the secondary storage used for longer term sotrage. The script *excludes*
# the actual data, as only the fastq files are stored on secondary storage.

# The MiSeq is not handled by this script, because we use the internal MiSeq
# storage at NSC. MiSeq copying is done by an internal cron job (TBD whether
# runs should first be copied on primary storage).

import os.path, sys
import argparse
import subprocess
import datetime


from genologics.lims import *
import nsc
import utilities


hiseq_exclude_paths = ["/Thumbnail_Images",
        "/Data/Intensities/L00*",
        "/Data/Intensities/BaseCalls/L00*"
        ]

nextseq_exclude_paths = ["TODO" ]


def rsync(source_path, destination_path, exclude):
    '''Runs the rsync command. Note that trailing slashes
    on paths are significant.'''

    args = [nsc.RSYNC, '-rlt', '--chmod=g+rwX']
    # TODO check if group is set by set-gid bit
    #args += ['--groupmap=*:' + nsc.SET_GROUP]
    args += ["--exclude=" + path for path in exclude]
    args += [source_path, destination_path]
    code = subprocess.check_call(args)
    return code


def main(process_id, instrument):

    process = Process(nsc.lims, id = process_id)
    process.udf['Status'] = 'Running...'
    process.put()

    seq_process = utilities.get_sequencing_process(process)
    runid = seq_process.udf['Run ID']

    destination = nsc.SECONDARY_STORAGE
    source = os.path.join(nsc.PRIMARY_STORAGE, runid) # No trailing slash
    if instrument == "hiseq":
        exclude = hiseq_exclude_paths
    elif instrument == "nextseq":
        exclude = nextseq_exclude_paths
    elif instrument == "miseq":
        print "Miseq not supported yet!"
    command_ok = rsync(source, destination, exclude)

    if command_ok:
        utilities.success_finish(process)
    else:
        utilities.fail(process, 'File copy error')
    return command_ok
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", help="Process ID", required=True)
    parser.add_argument('--instrument', help="Instrument type", required=True)

    args = parser.parse_args()

    main(args.pid, args.instrument)

