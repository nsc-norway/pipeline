# Copy global run stats/metadata files from primary to secondary storage

# This script copies data from the storage are written to by the sequencers,
# to the secondary storage used for longer term sotrage. The script *excludes*
# the actual data in BCL files, as only the fastq files are stored on secondary 
# storage. For HiSeq, we also attempt to exclude pre-existing fastq files.

# The MiSeq is not handled by this script, because we use the internal MiSeq
# storage at NSC. MiSeq copying is done by an internal cron job (TBD whether
# runs should first be copied on primary storage).

import os.path, sys
import argparse
import subprocess
import datetime


from genologics.lims import *
from common import nsc, utilities


hiseq_exclude_paths = [
        "/Thumbnail_Images",
        "/Data/Intensities/L00*",
        "/Data/Intensities/BaseCalls/L00*",
        "/Unaligned*"
        ]

nextseq_exclude_paths = [
        "/Data/Intensities/L00*",
        "/Data/Intensities/BaseCalls/L00*",
        ]

miseq_exclude_paths = [
        "/Thumbnail_Images",
        "/Data/Intensities/L00*",
        "/Data/Intensities/BaseCalls/L00*"
        ]


def rsync(source_path, destination_path, exclude):
    """Runs the rsync command. Note that trailing slashes
    on paths are significant."""

    # Note "-L" (copy links as file) -- gives us some flexibility to 
    # symlink source run, and it will still make a real copy.
    # We don't expect any symlinks in the run directories, as it's written
    # by a Windows machine.
    args = [nsc.RSYNC, '-rLt', '--chmod=ug+rwX,o-rwx'] # or chmod 660
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


def copy_files(runid, instrument):
    destination = nsc.SECONDARY_STORAGE
    source = os.path.join(nsc.PRIMARY_STORAGE, runid) # No trailing slash
    if instrument == "hiseq":
        exclude = hiseq_exclude_paths
    elif instrument == "nextseq":
        exclude = nextseq_exclude_paths
    elif instrument == "miseq":
        exclude = miseq_exclude_paths
    return rsync(source, destination, ["/" + runid + e for e in exclude])


