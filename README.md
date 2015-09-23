# pipeline

Scripts for initial processing of sequence data.

See the [Wiki](https://github.com/nsc-norway/pipeline/wiki) for more information. 


* [Demultiplexing](https://github.com/nsc-norway/pipeline/wiki/Demultiplexing)
* [NSC's data processing manager](https://github.com/nsc-norway/pipeline/wiki/DataProcessing)


See SETUP.md for setup instructions. (currently out of date)

## Content

Elementary scripts:
* `10_copy_run.py` - 


### Command line usage

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



