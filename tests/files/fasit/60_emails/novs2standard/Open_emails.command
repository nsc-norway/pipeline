#!/bin/bash

# This Mac shell script is hard-linked into the QualityControl/Delivery folder in
# all runs processed by the demultiplexing scripts. Original location:
# /data/runScratch.boston/scripts. It calls the AppleScript file
# "Open Emails.scpt", which is not linked and just located in 
# /data/runScratch.boston/scripts.


SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
osascript /Volumes/runScratch/scripts/Open\ Emails.scpt "$SCRIPT_DIR"
