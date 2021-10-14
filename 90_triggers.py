# Run triggers for completed run

# General interface for running additional programs automatically.
# Triggers are located in nsc.TRIGGER_DIR (see common/nsc.py for site-
# dependent location). Each trigger must be an executable file. 

####################
# PROJECT TRIGGERS #
####################

# Trigger files matching the glob pattern:
# project.<PROJECT_NAME>.*
# Will launch once for each instance of PROJECT_NAME in the completed run.
# Replace <PROJECT_NAME> with a match string for the project name.  PROJECT_NAME
# is matched partially from the beginning of the project name. 
# The remainder of the scripts's name, represented by *, is ignored, and should
# usually contain a hint to what the trigger is for.
# Example: a trigger named "project.Olsen-.encryption.sh"
# will be run for the project Olsen-excap3-2015-04-15, but a trigger named
# project.Olsen-RNA.alignment.sh will not.

# project..* 
# Run once for every single project, potentially many times per run. 

# PARAMETERS:

# The triggers are called with the following parameters:

#  PROJECT_PATH SAMPLE_SHEET_PATH PROJECT_NAME RUN_ID [PROCESS_ID]

# PROJECT_PATH: path to the directory containing fastq files.
# SAMPLE_SHEET_PATH: path to the sample sheet used for demultiplexing,
#   i.e. the pre-processed "DemultiplexingSampleSheet.csv" created by
#   20_prepare_sample_sheet.py
#   (note: there project directory may not contain all the lanes 
#   specified in the sample sheet, if the demultiplexing was only
#   run on a subset of the lanes. see --lanes option)
#   Note2: This requires that the prepare sample sheet and demultiplexing 
#   scripts were run in the same way -- LIMS or not -- as this script.
#   If not, the sample sheet file will not exist at this path.
# PROJECT_NAME: name of the project
# RUN_ID: run ID
# PROCESS_ID: LIMS-ID of the demultiplexing process. Omitted if 
#   not applicable.


################
# RUN TRIGGERS #
################

# run.<RUN_ID>.*
# Trigger is run if the run ID matches RUN_ID. Same semantics as above,
# partial match, if RUN_ID matches the start of the run ID, the trigger 
# is run.

# run..*
# Run once for every run.

# PARAMETERS:

#  RUN_PATH SAMPLE_SHEET_PATH RUN_ID [PROCESS_ID]

# RUN_PATH: The path to the run directory. If demultiplexing into a 
#   different directory than that which is written by the sequencer,
#   this is the directory containing the fastq files.
# Other parameters have the same meaning as for projects.

###############
# MORE INFO   #
###############

# LOGGING

# A log is created for each trigger in the DemultiplexLogs directory.
# The name is based on the name of the trigger. If a trigger is run 
# multiple times, the log is appended to for each execution

# ERRORS

# Error exit codes are silently ignored. If a script exits with an 
# error, it may be run again if it matches multiple projects.

# EXECUTION CONTEXT

# The triggers are run as the automatic demutliplexing user. Triggers
# are executed serially, and also block some of the last "official"
# processing scripts. For this reason they should be short.
# If launching longer compute jobs, do it via nohup, slurm, or similar.


import os
import re
import glob
import subprocess
from common import nsc, taskmgr

TASK_NAME = "90. Triggers"
TASK_DESCRIPTION = """Trigger custom scripts."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'lanes']


def main(task):
    task.running()

    runid = task.run_id
    if task.work_dir.startswith(nsc.SECONDARY_STORAGES['Diagnostics']):
        # Skip this for diag projects
        task.success_finish()
        return
    projects = (project for project in task.projects if not project.is_undetermined)

    project_triggers = glob.glob(os.path.join(nsc.TRIGGER_DIR, "project.*.*"))
    project_match_strings = [re.match(r"project\.(.*?)\.", os.path.basename(pt)).group(1) for pt in project_triggers]
    run_triggers = glob.glob(os.path.join(nsc.TRIGGER_DIR, "run.*.*"))

    run_id = task.run_id

    # Required info for parameters
    if task.process: # LIMS mode
        sample_sheet_path = os.path.join(
                task.work_dir,
                "DemultiplexingSampleSheet-" + task.process.id + ".csv"
                )
    else:
        sample_sheet_path = task.sample_sheet_path


    match_run_triggers = [rt for rt in run_triggers if run_id.startswith(re.match(r"run\.(.*?)\.", os.path.basename(rt)).group(1))]
    if match_run_triggers:

        task.info("Running global triggers...")
        args = [task.work_dir, sample_sheet_path, task.run_id]
        if task.process: # LIMS mode
            args += task.process.id

        for rt in run_triggers:
            basename = os.path.basename(rt)
            logfile = open(task.logfile(basename), "a")
            ret = subprocess.call([rt] + args, stdout=logfile, stderr=logfile)

    for project in projects:
        match_triggers = [pt for pt, pms in zip(project_triggers, project_match_strings) if project.name.startswith(pms)]
        if match_triggers:
            args = [
                    os.path.join(task.bc_dir, project.proj_dir),
                    sample_sheet_path,
                    project.name,
                    task.run_id
                ]
            if task.process:
                args.append(task.process.id)

            task.info("Running triggers for project " + project.name + "...")
            for trigger in match_triggers:
                basename = os.path.basename(trigger)
                logfile = open(task.logfile(basename), "a")
                ret = subprocess.call([trigger] + args, stdout=logfile, stderr=logfile)

    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

