#!/bin/sh

# Note: These must be in order for the process to continue
/usr/bin/python /data/nsc.loki/automation/pipeline/40_copy_sav_files.py --pid=$1
/usr/bin/python /data/nsc.loki/automation/pipeline/40_move_results.py --pid=$1

