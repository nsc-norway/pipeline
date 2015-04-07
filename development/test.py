#q = Queue(nsc.lims, id="40")
#inputs = q.artifacts[0:1]

#step = nsc.lims.create_step(q.protocol_step_config, dict((i,1) for i in inputs))
#step = Process(nsc.lims, id = "24-")

import nsc
from genologics.lims import *
import qc


