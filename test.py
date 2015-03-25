#q = Queue(nsc.lims, id="40")
#inputs = q.artifacts[0:1]

#step = nsc.lims.create_step(q.protocol_step_config, dict((i,1) for i in inputs))
#step = Process(nsc.lims, id = "24-")

import demultiplex
print demultiplex.parse_demux_stats(open("/home/fa2k/demo/demux-results/Basecall_Stats_C6A17ANXX/Demultiplex_Stats.htm").read())

