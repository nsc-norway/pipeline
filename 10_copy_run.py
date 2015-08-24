# Copy global run stats/metadata files from primary to secondary storage

# This script copies data from the storage are written to by the sequencers,
# to the secondary storage used for longer term sotrage. The script *excludes*
# the actual data in BCL files, as only the fastq files are stored on secondary 
# storage.

# For the MiSeq, this also copies the demultiplexed data.

import os
import sys

from genologics.lims import *
from common import nsc, utilities, slurm, taskmgr

TASK_NAME = "Copy run"
TASK_DESCRIPTION = "Copy run metadata"
TASK_ARGS = ['src_dir', 'work_dir']

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
        "/Data/Intensities/L00*",
        "/Data/Intensities/BaseCalls/L00*"
        ]


def rsync_arglist(source_path, destination_path, exclude):
    """Runs the rsync command. Note that trailing slashes
    on paths are significant."""

    # Note "-L" (copy links as file) -- gives us some flexibility to 
    # symlink source run, and it will still make a real copy.
    # We don't expect any symlinks in the run directories, as it's written
    # by a Windows machine.

    # No "copy source permissions" setting. Using default umask for
    # seq-user which is set via sudoers. Then use chmod to make sure that
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
    """To be run from LIMS on the NSC data processing step"""

    task.running()
    runid = task.run_id


    source = task.src_dir
    destination = task.work_dir    
    # Check destination
    print runid
    instr_from_dest = utilities.get_instrument_by_runid(runid)
    if not instr_from_dest:
        task.fail("Destination does not look like an Illumina run folder", 
                """Remember to include the name of the destination directory in the 
work_dir argument.""")


    # Specify source with trailing slash to copy content
    source = source.rstrip('/') + "/"

    instrument = utilities.get_instrument_by_runid(runid)
    if instrument == "hiseq":
        exclude = hiseq_exclude_paths
    elif instrument == "nextseq":
        exclude = nextseq_exclude_paths
    elif instrument == "miseq":
        exclude = miseq_exclude_paths

    args = rsync_arglist(source, destination, exclude)
    srun_args = ["--nodelist=loki"] # obviously OUS specific, but the whole script may be
    
    if task.process:
        # Can't use a per-run log dir, as it's not created yet, it's 
        # created by the rsync command
        logfile = os.path.join(nsc.LOG_DIR, task.process.id + "-rsync.txt")
        job_name = task.process.id + "." + TASK_NAME
    else:
        logfile = None
        job_name = TASK_NAME

    rc = slurm.srun_command(
            args, job_name, "02:00:00", logfile=logfile, srun_args=srun_args
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


with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
    main(task)

