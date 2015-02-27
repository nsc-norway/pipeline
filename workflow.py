#------------------------------#
# Workflow management          #
#------------------------------#

# Manages workflow steps in the LIMS. This mainly centers around the 
# step API resource. Advancing work etc.


import logging
from genologics.lims import *

logger = logging.getLogger()


def finish_step(lims, process_id):
    '''Sets next step for all analytes and finishes the specified step.
    This function can handle steps which have a single configured next
    step, and steps which are the last step of a protocol.
    All samples are advanced to the next step.'''

    step = Step(id=process_id)

    finish_protocol = False
    next_step = None

    for a in step.actions.next_actions:
        if finish_protocol:
            a.action = "???"
        else:
            a.next_step = next_step
            a.action = "nextstep"




def start_step_move():
    '''Starts a step with the specified samples and moves to the 
    "record details" screen.'''
    pass

