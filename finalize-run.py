# Finalise processing of run

# Moves run directory into processed/ on primary storage
# Using a slurm job is extreme overkill for what it does now,
# but it's convenient to have the same setup as the other jobs.

import sys
import os
import subprocess
from genologics.lims import *
from common import nsc, parse, utilities


def main(process_id):
    process = Process(nsc.lims, id=process_id)
    utilities.running(process)

    inputs = process.all_inputs(unique=True)
    flowcells = set(i.location[0] for i in inputs)
    if len(flowcells) == 1 and \
            len(inputs) == next(iter(flowcells)).occupied_wells:

        fc = next(iter(flowcells))
        del fc.udf[nsc.AUTO_FLOWCELL_UDF]
        fc.put()

        seq_process = utilities.get_sequencing_process(process)

        runid = seq_process.udf['Run ID']
        print "Moving", runid, "to processed directory"
        os.rename(
                os.path.join(nsc.PRIMARY_STORAGE, runid),
                os.path.join(nsc.PRIMARY_STORAGE, "processed", runid)
                )

        utilities.success_finish(process)
    else:
        utilities.fail(process, "Need all inputs of a single flowcell as input")


try:
    main(sys.argv[1])
except:
    if len(sys.argv) > 1:
        process = Process(nsc.lims, id=sys.argv[1])
        utilities.fail(process, "Unexpected: " + str(sys.exc_info()[1]))
    raise
    

