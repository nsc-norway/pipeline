#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
osascript /Volumes/runScratch/scripts/Open\ Emails.scpt "$SCRIPT_DIR"
