from genologics.lims import *
import nsc

#q = Queue(nsc.lims, id="40")
#inputs = q.artifacts[0:1]

#step = nsc.lims.create_step(q.protocol_step_config, dict((i,1) for i in inputs))
step = Step(nsc.lims, id = "24-312")
