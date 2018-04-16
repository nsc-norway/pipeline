# Extra copy step for NextSeq. 

# This copies the run from the NextSeq again, because the NextSeq will signal
# that the run is finished before it's done washing. So there's a few log files
# we're missing out on.

import os
import sys
import time

from genologics.lims import *
from common import nsc, utilities, taskmgr, remote

TASK_NAME = "95. Copy run again (NextSeq)"
TASK_DESCRIPTION = "Check for completion and copy run metadata again"
TASK_ARGS = ['src_dir', 'work_dir', 'lanes']

nextseq_exclude_paths = [
        "/Thumbnail_Images",
        "/Data/Intensities/L00*",
        "/Data/Intensities/BaseCalls/L00*",
        ]


def rsync_arglist(source_path, destination_path, exclude):
    """Runs the rsync command. Note that trailing slashes
    on paths are significant."""
    
    # See comments on rsync in 10_copy_run.py
    args = [nsc.RSYNC, '-rLt', '--chmod=ug+rwX']
    args += ["--exclude=" + path for path in exclude]
    args += [source_path, destination_path]
    return args



def main(task):
    """To be run from LIMS on the NSC data processing step"""

    task.running()
    runid = task.run_id


    source = task.src_dir
    destination = task.work_dir    

    # Specify source with trailing slash to copy content
    source = source.rstrip('/') + "/"

    # Check destination
    instrument = utilities.get_instrument_by_runid(runid)
    if not instrument:
        task.fail("Destination does not look like an Illumina run folder", 
                """Remember to include the name of the destination directory in the 
work_dir argument.""")

    if instrument == "nextseq":
        exclude = nextseq_exclude_paths
    else:
        task.info("This is a no-op for other sequencers than NextSeq.")
        task.success_finish()

    while not os.path.exists(os.path.join(source, "RunCompletionStatus.xml")):
        task.info("Waiting for RunCompletionStatus.xml...")
        time.sleep(60)


    task.info("Copying remaining files...")
    args = rsync_arglist(source, destination, exclude)
    
    logfile=task.logfile("rsync-2")

    rc = remote.run_command(
            args, task, "copy_run_again", "00:05:00", logfile=logfile, 
            storage_job=True, comment=runid
            )
    
    if rc == 0:
        task.success_finish()
    else:
        try:
            detail = open(logfile).read()
        except IOError:
            detail = None
        task.fail("rsync failed", detail)


with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
    main(task)

