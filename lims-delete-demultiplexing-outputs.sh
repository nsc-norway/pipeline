#!/bin/bash

DIR=`echo "$1" | head -n1`
if echo "$DIR" | grep -x '/data/runScratch\.boston/demultiplexed/[1-9][0-9]*_[^/]*/*' > /dev/null
then
    if [ -d "$DIR" ]
    then
        rm -rf $DIR/Data/Intensities/BaseCalls/{Reports,Stats,QualityControl}
        rm -rf $DIR/Data/Intensities/BaseCalls/{Reports,Stats,QualityControl}_L00*
        rm -rf $DIR/Data/Intensities/BaseCalls/*.Project_*
        rm -rf $DIR/Data/Intensities/BaseCalls/multiqc_*
        rm -rf $DIR/Data/Intensities/BaseCalls/Undetermined_*.fastq.gz
        rm -rf $DIR/Data/Intensities/BaseCalls/*.qc.pdf
        echo "Successfully deleted the outputs."
    else
        echo "Error: '$1' is not a directory."
        exit 1
    fi
else
    echo "Error: specified directory '$1' is not acceptable."
    exit 1
fi

