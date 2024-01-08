# This script can be used to dump a json representation of the
# project objects associated with a sample sheet specified on 
# the command line.

# The json-encoded representation can be used as a reference
# for targeted tests of the sample sheet parsing.

# Settings for the sample sheet parser can be set
# using environment variables. See example.

# USAGE:
# python dump_projects_json.py SAMPLE_SHEET_PATH RUN_ID

# EXAMPLE:
# READS=1 COLLAPSE_LANES=true python dump_projects_json.py SampleSheet.csv 100000_RTEST


# READS:            Number of data reads, usually 1=single read, 2=paired end.
# COLLAPSE_LANES:   Set to true if data from all lanes are combined into a single
#                   fastq file (bcl2fastq option --no-lane-splitting).
# EXPAND_LANES:     Set to a string containing the lane numbers, e.g. 1234. When
#                   there is no Lane column in the sample sheet, this will add
#                   file entries for the specicfied lanes. By default "1", but
#                   not used if Lane is included in sample sheet.
# ADD_INDEX_FILES:  Set to true to add index FASTQ files I1 and I2 to the list
#                   of files.  Index files are produced by the bcl2fastq option
#                   --create-fastq-for-index-reads. They are only included in
#                   the MD5 sum and delivery, not the QC. They are only included
#                   in the projects data structure if
#                   samples.add_index_read_files() is called in the script under
#                   test.

import sys
import os
import json
sys.path.append('..')
from common import samples, nsc
from test_module import projects_to_dicts

if len(sys.argv) < 3:
    print("Please see script code for usage information (require 2 arguments)")
    sys.exit(1)

collapse_lanes = os.environ.get('COLLAPSE_LANES') == 'true' # This setting turns lane number into "X"
expand_lanes = [int(c) for c in os.environ.get('EXPAND_LANES', '1')]
reads = int(os.environ.get('READS', 2)) # Number of data reads; 1=single read, 2=paired end

with open(sys.argv[1]) as sample_sheet_file:
    sample_sheet_content = sample_sheet_file.read()
sample_sheet = samples.parse_sample_sheet(sample_sheet_content)
sample_sheet_data = sample_sheet['data']
projects = samples.get_projects(sys.argv[2], sample_sheet_data, reads, collapse_lanes, expand_lanes)
if os.environ.get('ADD_INDEX_FILES'):
    samples.add_index_read_files(projects, "/dev/null", True)
print((json.dumps(projects_to_dicts(projects))))

