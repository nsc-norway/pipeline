#!/bin/bash

# Create fastq files in new dir with one byte gziped

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

