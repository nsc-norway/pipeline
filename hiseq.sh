#!/bin/sh

set -e

SOURCE=$1
DEST=$2

DIR=`dirname $0`

if [[ -d "$SOURCE" && ! -d "$DEST" && -d `dirname $DEST` ]]
then

	python $DIR/10_copy_run.py $SOURCE $DEST
	python $DIR/20_prepare_sample_sheet.py $DEST
	python $DIR/30_demultiplex.py $SOURCE $DEST

	SCRIPTS="40_move_results.py 50_emails.py 60_fastqc.py 70_reports.py 80_md5sum.py"

	for script in $SCRIPTS
	do
		python $(dirname $0)/$script $DEST
	done
else
	echo "use: hiseq.sh SOURCE-RUN DEST-RUN"
fi
