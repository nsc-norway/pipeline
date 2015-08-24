#!/bin/sh

set -e

SCRIPTS="50_emails.py 50_update_lims.py 60_fastqc.py 70_reports.py 80_md5sum.py 90_prepare_delivery.py"

if [ -z "$1" ]
then
	echo "Use: lims-qc.sh <PROCESS_ID>"
	exit 1
fi

for script in $SCRIPTS
do
	python $(dirname $0)/$script --pid=$1
done

