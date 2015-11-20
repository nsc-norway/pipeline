#!/bin/sh

set -e

SCRIPTS="50_emails.py 60_fastqc.py 70_reports.py 70_demultiplex_stats.py 80_md5sum.py"

if [ -z "$1" ]
then
	echo "Use: run-qc.sh [OPTIONS] <WORK-DIR>"
	exit 1
fi

for script in $SCRIPTS
do
	python $(dirname $0)/$script "$@"
done

