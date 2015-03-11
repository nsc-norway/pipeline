from genologics.lims import *
import nsc

#q = Queue(nsc.lims, id="40")
#inputs = q.artifacts[0:1]

#step = nsc.lims.create_step(q.protocol_step_config, dict((i,1) for i in inputs))
#step = Process(nsc.lims, id = "24-")

art = Process(nsc.lims, id="24-2540")

pf = ProtoFile(nsc.lims, attached_to = art.uri, original_location="/dev/null")
pf2=nsc.lims.glsstorage(pf)
f = pf2.post()
f.upload("FORE!")
print f.id
