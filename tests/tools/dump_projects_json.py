# This script can be used to dump a json representation of the
# project objects associated with a sample sheet specified on 
# the command line.

# The json-encoded representation can be used as a reference
# for targeted tests of the sample sheet parsing.

# Default settings for the sample sheet parser can be set
# using environment variables. See example.

# USAGE:
# python dump_projects_json.py SAMPLE_SHEET_PATH RUN_ID

# EXAMPLE:
# READS=1 COLLAPSE_LANES=true python dump_projects_json.py SampleSheet.csv 100000_RTEST

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
reads = int(os.environ.get('READS', 2)) # Number of data reads; 1=single read, 2=paired end

with open(sys.argv[1]) as sample_sheet_file:
    sample_sheet_content = sample_sheet_file.read()
sample_sheet = samples.parse_sample_sheet(sample_sheet_content)
sample_sheet_data = sample_sheet['data']
projects = samples.get_projects(sys.argv[2], sample_sheet_data, reads, collapse_lanes)
print(json.dumps(projects_to_dicts(projects)))

