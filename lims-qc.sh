#!/bin/sh

set -e

SCRIPTS="50_qc_analysis.py 60_emails.py 60_update_lims.py 60_reports.py 60_demultiplex_stats.py 70_multiqc.py 80_md5sum.py"

if [ -z "$1" ]
then
	echo "Use: lims-qc.sh <PROCESS_ID>"
	exit 1
fi

for script in $SCRIPTS
do
	/usr/bin/python $(dirname $0)/$script --pid=$1
done

