# This script can be used to dump a json representation of the
# project objects associated with a sample sheet specified on 
# the command line.

# The json-encoded representation can be used as a reference
# for targeted tests of the sample sheet parsing.

# Default settings for the sample sheet parser are provided, and
# you can alter the parameters in this code if other settings
# are required.

import sys
import json
sys.path.append('..')
from common import samples, nsc
from test_module import projects_to_dicts

with open(sys.argv[1]) as sample_sheet_file:
    sample_sheet_content = sample_sheet_file.read()
sample_sheet = samples.parse_sample_sheet(sample_sheet_content)
sample_sheet_data = sample_sheet['data']
projects = samples.get_projects("180502_NS500336_001_ANOINDEX",
        sample_sheet_data, 2, True)
print(json.dumps(projects_to_dicts(projects)))

