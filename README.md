# pipeline

Scripts for initial processing of sequence data.

See the [Wiki](https://github.com/nsc-norway/pipeline/wiki) for more information. 


* [Demultiplexing](https://github.com/nsc-norway/pipeline/wiki/Demultiplexing)
* [NSC's data processing manager](https://github.com/nsc-norway/pipeline/wiki/DataProcessing)


See SETUP.md for setup instructions. (currently out of date)

## Content

### Main scripts: the primary data processing / analysis building blocks.
* `10_copy_run.py` - Copy run folder excluding the actual data. Used to keep the demultiplexed data in a different place than the BCLs (original data).
* `20_prepare_sample_sheet.py` - Modify sample sheets created by LIMS
  * Remove special characters
  * Do a reverse-complement of NextSeq index2 column
  * Add headers to HiSeq sample sheet, to work with bcl2fastq2
* `30_demultiplexing.py` - Calls bcl2fastq2
* `40_move_results.py` - Moves the demultiplexing results into the NSC standard file/directory naming
* `50_emails.py` - Produces some reports used for sending delivery emails
* `60_fastqc.py` - Runs FastQC and moves the output into project / sample specific directories
* `70_reports.py` - Produces QC PDF files for the user and a HTML fastqc overview page
* `70_demultiplex_stats.py` - Generate Demultiplex_stats.htm for each project for compatibility with bcl2fastq version 1.x (e.g. for users' downstream scripts).
* `80_md5sum.py` - Computes checksum of fastq files and PDF reports
* `90_prepare_delivery.py` - Various site-specific data management operations, getting the data into a format which is easy to deliver. Uses the LIMS project metadata to get the delivery type information.
* `95_copy_run_check.py` - We get the "run complete" signal from NextSeq (via LIMS) before the RTA has finished writing some logs. This script does the same as `10_copy_run.py` to copy these last logs, but also check for a file which indicates that the NextSeq has finished completely.
* `99_processed.py` - Adds some information to LIMS which makes it easier to keep track of finished runs. Moves the run folder into a subdirectory called "processed".
* `demultiplex_stats.py` - Supporting module to generate `Demultiplex_stats.htm`, but can also be invoked directly to do this (usually better to use `70_demultiplex_stats.py` to do it for all projects)

### LIMS integration, automation: scripts to manage LIMS processes / workflow.
* `sequencing-to-demultiplexing.py` - A button to be added to the sequencing step, to add a run to demultiplexing.
* `lims-setup-step.py` - Called when entering the demultiplexing step, to set the source / destination folders and get the sample sheet from the cluster generation step.
* `auto.py` - Automatic "button clicker" to continue calling next script when tasks complete. Runs in cron job.

### Batch scripts
* `lims-qc.sh` - QC scripts combined into one, for use on the QC button in LIMS, to reduce the number of buttons. Called with the process-ID.
* `run-qc.sh` - Run all QC scripts, used in command-line mode.
* `hiseq.sh`, `miseq.sh` - Batch command-line processing of runs, includes all commands relevant for the sequencers for non-LIMS mode.

### Common library modules (common/)
Modules used by multiple scripts:
* `common/Counter.py` - Python 2.6 compat.
* `common/__init__.py` - Package.
* `common/lane_info.py` - Getting lane / flowcell statistics from sequencing runs.
* `common/nsc.py` - Configuration file.
* `common/remote.py` - Remote command execution interface, supports srun and local.
* `common/samples.py` - Getting sample-sheet information and representing it as Python objects. Also computes various paths for naming data and QC files.
* `common/secure_dummy.py` - Dummy versions of securiy-sensitive functions which shouldn't be in git.
* `common/stats.py` - Parsing demultiplexing stats XML files.
* `common/taskmgr.py` - Manages execution, status messages, error reporting. Provides an interface to information which is fetched differently in LIMS mode and command-line mode.
* `common/utilities.py` - Functions of general utility.

### Other
* `deploy/`: Deployment scripts (simple).
* `docs/`: Some documentation, not very relevant / up to date
* `monitor/`: Flask-based web front-end to monitor sequencing and demultiplexing processes ("Overview" page).
* `template/`: Template files used by QC reporting scripts




## Command line usage

The scripts support being called from the command line, and follow a similar, but
not identical code path as when triggered by the LIMS. Not all scripts can be run
outside LIMS - currently `50_update_lims.py` and `90_prepare_delivery.py` will not 
run outside the LIMS, and `99_processed.py` will not do anything interesting.

### Argument overview

The elementary scripts are the ones named `NN_xxxx.py` where NN is a number. Then there are
shell scripts to combine multiple of these elementary scripts, so one doesn't have to invoke 
them one by one (described below).

The required arguments are the source run directory and the "working" run directory
(destination). The source run is only required for `10_copy_run.py`, `30_demultiplexing.py`
and `95_copy_run_check.py`, and may be the same as the working directory (if it's the same,
then `10_copy_run` is a no-op and should be skipped). To determine what arguments can be
used with a script, call it with the -h option.


### Batch scripts



