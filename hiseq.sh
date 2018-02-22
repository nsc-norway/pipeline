#!/bin/sh

set -e

LANES=""
THREADS=""
EXTRA_OPTIONS=""

while [[ "$1" == --* ]]
do
	if [[ "$1" == --lanes=* ]]
	then
		LANES=$1
	elif [[ "$1" == --extra-options=* ]]
	then
		EXTRA_OPTIONS=$1
	elif [[ "$1" == --threads=* ]]
	then
		THREADS=$1
	fi
	shift
done

SOURCE=$1
DEST=$2

if [[ -z ""$SOURCE"" ]]
then
	echo "Use: hiseq.sh [--lanes=XYZ] [--extra-options=OPTIONS] [--threads=N] SOURCE_DIR [DESTINATION_DIR]"
	exit 1
fi

if [[ -z ""$DEST"" ]]
then
    DEST="$SOURCE"
fi

DIR=`dirname $0`

nsc-python27 $DIR/10_copy_run.py "$SOURCE" "$DEST"
nsc-python27 $DIR/20_prepare_sample_sheet.py $LANES "$DEST"
if [ -z "$EXTRA_OPTIONS" ]
then
	nsc-python27 $DIR/30_demultiplexing.py $THREADS $LANES "$SOURCE" "$DEST"
else
	nsc-python27 $DIR/30_demultiplexing.py $THREADS "$EXTRA_OPTIONS" $LANES "$SOURCE" "$DEST"
fi
nsc-python27 $DIR/40_move_results.py $LANES "$DEST"
nsc-python27 $DIR/50_qc_analysis.py $THREADS $LANES "$DEST"
nsc-python27 $DIR/60_emails.py $LANES "$DEST"
nsc-python27 $DIR/60_reports.py $LANES "$DEST"
nsc-python27 $DIR/80_md5sum.py $THREADS $LANES "$DEST"
nsc-python27 $DIR/70_multiqc.py $LANES "$DEST"
nsc-python27 $DIR/90_prepare_delivery.py $LANES "$DEST"

