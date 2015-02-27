#------------------------------#
# Workflow management objects  #
#------------------------------#

# Manages workflow steps in the LIMS. This mainly centers around the 
# step API resource. Advancing work etc.

# The SciLife library doesn't provide the access we need to workflow
# management objects.

import logging
from genologics.lims import *

logger = logging.getLogger()

class Step(Entity):
    "Step instance: a protocol step, alternative representation of a process"

    _URI = 'steps'

    
    
