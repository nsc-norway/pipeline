# Move run into processed/ on primary storage, and mark as completed
# in LIMS 

import sys
from genologics.lims import *
from common import nsc

def main(process_id):
    if ':' in process_id:
        server_id, process_id = process_id.split(":")[0:2]
    else:
        server_id, process_id = None, process_id
    lims = nsc.get_lims(server_id)
    process = Process(lims, id=process_id)
    inputs = process.all_inputs(unique=True, resolve=True)
    lims_samples = lims.get_batch(set(sample for i in inputs for sample in i.samples))
    for lims_project in set(sample.project for sample in lims_samples):
        # Have to check for existence; controls don't have project
        if lims_project and lims_project.udf.get('Project type') in ['FHI-Covid19', 'MIK-Covid19']:
            lims_project.close_date = datetime.date.today()
            lims_project.put()

if __name__ == "__main__":
    main(sys.argv[1])

