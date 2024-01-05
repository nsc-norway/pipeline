# Copy global run stats/metadata files from primary to secondary storage

# This script copies data from the storage are written to by the sequencers,
# to the secondary storage used for longer term sotrage. The script *excludes*
# the actual data in BCL files, as only the fastq files are stored on secondary 
# storage.

# For the MiSeq, the data demultiplexed on the machine are also excluded.

import os
import sys
import time

from genologics.lims import *
from common import nsc, utilities, remote, taskmgr

TASK_NAME = "10. Copy run"
TASK_DESCRIPTION = "Copy run metadata"
TASK_ARGS = ['src_dir', 'work_dir', 'lanes']

hiseq_exclude_paths = [
        "/Thumbnail_Images",
        "/Data/Intensities/L00*",
        "/Data/Intensities/BaseCalls/L00*",
        ]

nextseq_exclude_paths = [
        "/Thumbnail_Images",
        "/Data/Intensities/L00*",
        "/Data/Intensities/BaseCalls/L00*",
        ]

miseq_exclude_paths = [
        "/Thumbnail_Images",
        "/Images",
        "/Data/Intensities/L00*",
        "/Data/Intensities/BaseCalls/L00*",
        "/Data/Intensities/BaseCalls/*.fastq.gz",
        ]


def rsync_arglist(source_path, destination_path, exclude):
    """Return a list of arguments to run the rsync command. 
        Note that trailing slashes on paths are significant."""

    # Note "-L" (copy links as file) -- gives us some flexibility to 
    # symlink source run, and it will still make a real copy.
    # We don't expect any symlinks in the run directories, as it's written
    # by Windows machines.

    # No "copy source permissions" setting. Using default umask.
    # Then use chmod argument to rsync to make sure that
    # all non-masked permissions are available. (this is the only way to 
    # make it work)
    args = [nsc.RSYNC, '-rLt', '--chmod=ug+rwX']
    args += ["--exclude=" + path for path in exclude]
    args += [source_path, destination_path]
    # Running rsync client in slurm jobs: It is necessary to remove SELinux protections
    # on rsync. This isn't a security hazard because the restrictions are intended for
    # the rsync daemon, not the client that we use. The command is:
    # sudo semanage fcontext -m -t bin_t /usr/bin/rsync
    # sudo restorecon /usr/bin/rsync
    # Confirm: ls -lZ /usr/bin/rsync | grep bin_t
    # Note: first command requires absolute path.
    return args



def main(task):
    
    task.running()
    run_id = task.run_id


    source = task.src_dir
    destination = task.work_dir    
    # Check destination
    print(run_id)
    instrument = utilities.get_instrument_by_runid(run_id)
    if not instrument:
        task.fail("Destination does not look like an Illumina run folder", 
                """Remember to include the name of the destination directory in the 
work_dir argument.""")

    first_location_source = source
    if not os.path.exists(source):
        # Check if the run exists on the default source (runScratch) and if so, change
        # the source, and move it once CopyComplete is done.
        check_path = os.path.join(nsc.PRIMARY_STORAGES['default'], run_id)
        if os.path.exists(check_path):
            first_location_source = check_path

    while not os.path.exists(os.path.join(first_location_source, "CopyComplete.txt")):
        task.info("Waiting for CopyComplete.txt...")
        time.sleep(60)

    if first_location_source != source:
        os.rename(first_location_source, source)

    # Specify source with trailing slash to copy content
    source = source.rstrip('/') + "/"

    if instrument in ["hiseq", "hiseqx", "hiseq4k", "novaseq"]:
        exclude = hiseq_exclude_paths
    elif instrument == "nextseq":
        exclude = nextseq_exclude_paths
    elif instrument == "miseq":
        exclude = miseq_exclude_paths

    args = rsync_arglist(source, destination, exclude)

    try:
        os.mkdir(destination)
    except OSError as e:
        if e.errno == 17: # Already exists
            pass
        else:
            raise

    logfile = task.logfile("rsync")
    rc = remote.run_command(
            args, task, "rsync", "02:00:00", logfile=logfile, comment=run_id
            )
    
    if rc == 0:
        task.success_finish()
    else:
        detail = None
        if task.process: #LIMS
            try:
                detail = open(logfile).read()
            except IOError:
                detail = None
        task.fail("rsync failed", detail)


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

