#------------------------------#
# Workflow management          #
#------------------------------#

# Manages workflow steps in the LIMS. This mainly centers around the 
# step API resource. Completing and starting steps.


import logging
import subprocess, sys
from genologics.lims import *

logger = logging.getLogger()


def finish_step(lims, process_id):
    '''Sets next step for all analytes and finishes the specified step.
    This function can handle steps which have a single configured next
    step, and steps which are the last step of a protocol.
    All samples are advanced to the next step.'''

    step = Step(lims, id=process_id)

    if step.current_state != 'Completed':

        next_elements = step.configuration.transitions
        finish_protocol = len(next_elements) == 0
        if len(next_elements) == 1:
            next_step = next_elements[0].step
        else:
            raise Exception("There are multiple options for next step, don't know what to do.")
    
        # Set next action for each sample
        for a in step.actions.next_actions:
            if finish_protocol:
                a.action = "complete"
            else:
                a.action = "nextstep"
                a.next_step = next_step
        step.actions.put()
    
        while step.current_state in ['Record Details', 'Assign Next Steps']:
            step.advance()
    
        if step.current_state != 'Completed':
            raise Exception("Failed to finish the step. It is in state " + step.current_state)


if sys.version_info >= (2, 7):
    check_output = subprocess.check_output
else:
    def check_output(args):
        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
        data = proc.communicate()[0]

        if proc.wait() == 0:
            return data
        else:
            raise subprocess.CalledProcessError("Non-zero exit code from " + args[0])



