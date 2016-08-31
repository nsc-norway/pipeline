# pipeline

Scripts for initial processing of sequence data.

See the [Wiki](https://github.com/nsc-norway/pipeline/wiki) for more information. 


* [Demultiplexing](https://github.com/nsc-norway/pipeline/wiki/Demultiplexing)
* [NSC's data processing manager](https://github.com/nsc-norway/pipeline/wiki/DataProcessing)


See SETUP.md for setup instructions. (currently out of date)

## Content

### Main scripts: the primary data processing / analysis tasks.
The scripts are named with a number prefix followed by a name. They are run in
the order of these numbers. The script lineup is frequently subject to change, so 
no documentation of each script is given here. At the top of each script, there's
a set of keywords about what it does, but for anything else you have to refer to
the code itself.

(note: `50_emails.py` script is not about sending emails)

### LIMS integration, automation: scripts to manage LIMS processes / workflow.
* `sequencing-to-demultiplexing.py` - A button to be added to the sequencing step, to add a run to demultiplexing.
* `lims-setup-step.py` - Called when entering the demultiplexing step, to set the source / destination folders and get the sample sheet from the cluster generation step.
* `auto-next-script.py` - Automatic "button clicker" to continue calling next script when tasks complete. Runs in cron job.

### Batch scripts
* `lims-qc.sh` - QC scripts combined into one, for use on the QC button in LIMS, to reduce the number of buttons. Called with the process-ID.
* `run-qc.sh` - Run all QC scripts, used in command-line mode.
* `hiseq.sh` - Command-line processing of runs, includes all commands relevant for the sequencers for non-LIMS mode.

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
* `template/`: Template files used by QC reporting scripts


## Command line usage

The scripts support being called from the command line, and follow a similar, but
not identical code path as when triggered by the LIMS. Not all scripts can be run
outside LIMS - currently `50_update_lims.py` and `90_prepare_delivery.py` will not 
run outside the LIMS, and `99_processed.py` will not do anything interesting.

### Argument overview

The required arguments to the `NN_xxxx.ppy` scripts 
are the source run directory and the "working" run directory
(destination). Not all scripts require source path.
To determine what arguments can be
used with a script, call it with the -h option.



