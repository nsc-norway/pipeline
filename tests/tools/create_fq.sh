#!/bin/bash

# Use: create_fq.sh SOURCE_RUN_PATH DEST_RUN_PATH

# Create fastq files in new dir with as byte gziped files, with names based on
# files in SOURCE_RUN_PATH. The paths to the root of the run folders can be
# used.

set -e

SOURCE_RUN=$1
DEST_RUN=`readlink -f $2`

pushd $SOURCE_RUN
for f in `find -type f -name \*.fastq.gz`
do
    FPATH=$DEST_RUN/$f
    mkdir -p `dirname $FPATH`
    echo '' | gzip > $FPATH
done
popd

