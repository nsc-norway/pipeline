# Copy global run stats/metadata files from primary to secondary storage

# Can be used as a module, or called directly from the command line. The latter
# is not used at the moment, and may not work properly.

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
from library import nsc, utilities


hiseq_exclude_paths = ["/Thumbnail_Images",
        "/Data/Intensities/L00*",
        "/Data/Intensities/BaseCalls/L00*"
        ]

nextseq_exclude_paths = ["/Data/Intensities/L00*",
        "/Data/Intensities/BaseCalls/L00*"]


def rsync(source_path, destination_path, exclude):
    """Runs the rsync command. Note that trailing slashes
    on paths are significant."""

    args = [nsc.RSYNC, '-rlt', '--chmod=g+rwX']
    args += ["--exclude=" + path for path in exclude]
    args += [source_path, destination_path]
    # Running rsync client in slurm jobs: It is necessary to remove SELinux protections
    # on rsync. This isn't a security hazard because the restrictions are intended for
    # the rsync daemon, not the client that we use. The command is:
    # sudo semanage fcontext -m -t bin_t /usr/bin/rsync
    # sudo restorecon /usr/bin/rsync
    # Confirm: ls -lZ /usr/bin/rsync | grep bin_t
    # Note: first command requires absolute path.
    code = subprocess.call(args)
    return code == 0


def copy_files(process, instrument):
    seq_process = utilities.get_sequencing_process(process)
    # if it throws KeyError, AttributeError, etc., tough luck, handle 
    # upstream
    runid = seq_process.udf['Run ID']

    destination = nsc.SECONDARY_STORAGE
    source = os.path.join(nsc.PRIMARY_STORAGE, runid) # No trailing slash
    if instrument == "hiseq":
        exclude = hiseq_exclude_paths
    elif instrument == "nextseq":
        exclude = nextseq_exclude_paths
    elif instrument == "miseq":
        print "Miseq not supported yet!"
    return rsync(source, destination, ["/" + runid + e for e in exclude])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", help="Process ID", required=True)
    parser.add_argument('--instrument', help="Instrument type", required=True)

    args = parser.parse_args()

    main(Process(nsc.lims, id=args.pid), args.instrument)
