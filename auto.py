#------------------------------#
# Conveyor
# NSC data processing manager
#------------------------------#

# This script monitors the LIMS via the REST API and starts programs.
# It only triggers scripts (emulates button presses) using the API to
# manage the processing status. 

import re

# scilife genologics library
from genologics.lims import *

# Local
from common import nsc, utilities


def start_programs():
    processes = nsc.lims.get_processes(type=nsc.DEMULTIPLEXING_QC_PROCESS)
    if not processes:
        return

    for process in processes:
        seq_process = utilities.get_
        try:
            state = process.udf[nsc.JOB_STATE_CODE_UDF]
        except KeyError:
            state = None

        if state == "COMPLETED":
            previous_program = process.udf[nsc.CURRENT_JOB_UDF]
        elif state == None:
            previous_program = None
        else:
            return # << return if state is not "COMPLETED" or None

            step = Step(nsc.lims, id=process.id)
            if step.program_status == None or step.program_status.status == "OK":
                auto_udf_match = [
                        re.match(r"Auto ([\d-]+\..*)", udfname)
                        for udfname, udfvalue in process.udf.items()
                        if udfvalue
                        ]
                auto_udf_name = sorted(m.group(1) for m in auto_udf_match if m)

                try:
                    next_program = next(
                            button_name for button_name in auto_udf_match
                            if button_name > previous_program
                            )
                
                    button = next(
                            program 
                            for program in step.available_programs
                            if program.name == next_program
                            )

                    button.trigger()

                except StopIteration:
                    pass # Can't find next checkbox or button


if __name__ == "__main__":
    start_programs()


