#!/bin/sh

set -e

SCRIPTS="50_qc_analysis.py 60_emails.py 60_reports.py 60_demultiplex_stats.py 70_multiqc.py 80_md5sum.py 90_prepare_delivery.py"

if [ -z "$1" ]
then
	echo "Use: run-qc.sh [OPTIONS] <WORK-DIR>"
	exit 1
fi

for script in $SCRIPTS
do
	nsc-python3 $(dirname $0)/$script "$@"
done

