# Finalise processing of run

# Moves run directory into processed/ on primary storage
# Using a slurm job is extreme overkill for what it does now,
# but it's convenient to have the same setup as the other jobs.

import sys
import os
import subprocess
from datetime import date
from genologics.lims import *
from common import nsc, parse, utilities


def finish_run(process, fc):
    fc.udf[nsc.RECENTLY_COMPLETED_UDF] = True
    fc.udf[nsc.PROCESSED_DATE_UDF] = date.today()
    fc.put()

    seq_process = utilities.get_sequencing_process(process)
    runid = seq_process.udf['Run ID']
    if all(lane.qc_flag == 'PASSED' for lane in fc.placements.values()):
        print "Moving", runid, "to processed directory"
        os.rename(
                os.path.join(nsc.PRIMARY_STORAGE, runid),
                os.path.join(nsc.PRIMARY_STORAGE, "processed", runid)
                )




def main(process_id):
    process = Process(nsc.lims, id=process_id)
    utilities.running(process)

    inputs = process.all_inputs(unique=True)
    flowcells = set(i.location[0] for i in inputs)
    if len(flowcells) == 1:
        fc = next(iter(flowcells))
        fc.get()
        try:
            del fc.udf[nsc.AUTO_FLOWCELL_UDF]
            automation = True
        except KeyError:
            automation = False

        # Don't run if not all QC pass lanes are here
        fc_lanes = fc.placements.values()
        ok_lanes = set(l for l in fc_lanes if l.qc_flag == 'PASSED')
        
        if set(inputs) >= ok_lanes:
            finish_run(process, fc)
            # Can't use the standard function to complete this step, as the automation
            # flag is already cleared above. Instead we call finish_step manually.
            utilities.success_finish(process, False)
            if automation:
                utilities.finish_step(nsc.lims, process.id)
        else:
            utilities.fail(process, "Need all QC passed inputs of a single flowcell as input")
    else:
        utilities.fail(process, "Only one flowcell allowed as input")


with utilities.error_reporter():
    main(sys.argv[1])

