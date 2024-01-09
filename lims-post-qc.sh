#!/bin/sh

set -e

SCRIPTS="90_prepare_delivery.py 90_triggers.py"

if [ -z "$1" ]
then
	echo "Use: lims-post-qc.sh <PROCESS_ID>"
	exit 1
fi

for script in $SCRIPTS
do
	nsc-python3 $(dirname $0)/$script --pid=$1
done

