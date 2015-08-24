#!/bin/sh

set -e

SCRIPTS="10_copy_run.py 20_prepare_sample_sheet.py 30_demultiplex.py 40_move_results.py 40_processed.py"

if [ -z "$1" ]
then
	echo "Use: lims-demultiplex.sh <PROCESS_ID>"
	exit 1
fi

for script in $SCRIPTS
do
	python $(dirname $0)/$script --pid=$1
done

