from genologics.lims import *
import nsc
step = Step(nsc.lims, id="122-308")
step.actions.next_actions[0].next_step=Step(nsc.lims, id="24-301")
step.actions.next_actions[0].action="nextstep"
print step.actions.next_actions[0]

