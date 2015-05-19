from flask import Flask, render_template
from genologics.lims import *
import nsc
import datetime
import threading
from functools import partial

# Project / Sample progress
# ---------------------------------

# Method for generating the progress overview:
# 1. Queues:   Queue resources are queried via the API to fetch 
#              samples which are queued for a step
# 2. Processes:Process types which should be monitored have a boolean 
#              UDF called "Monitor", with a default of true. This program
#              queries the API for any process of a given type, with the 
#              Monitor flag set. The Monitor flag is only used by this 
#              program. It is cleared if the process should no longer be
#              monitored.

app = Flask(__name__)

COMPLETED_DAYS = 30

INSTRUMENTS = ["HiSeq", "NextSeq", "MiSeq"]
# [ (Protocol, Step) ]
SEQUENCING = [
        ("Illumina SBS (HiSeq GAIIx) 5.0", "Illumina Sequencing (Illumina SBS) 5.0"),
        ("Illumina SBS (NextSeq) 1.0", "NextSeq Run (NextSeq) 1.0"),
        ("Illumina SBS (NextSeq) 1.0", "NextSeq Run (NextSeq) 1.0")
        ]

# [ (Protocol, (Step, Step, Step)) ]
DATA_PROCESSING = [
        ("NSC Data processing for HiSeq", (
            "NSC Demultiplexing (HiSeq)",
            "NSC Data Quality Reporting (HiSeq)"
            )),
        ]

queues = {}

def init_application():
    # We get queue instances as part of the initialisation, then keep them
    # in the queues dict. (looking up queues is time consuming)
    for protocol, protocol_steps in DATA_PROCESSING:
        proto = nsc.lims.get_protocols(name=protocol)[0]
        for step_name in protocol_steps:
            for ps in proto.steps:
                if step_name == ps.name:
                    queues[(protocol, step_name)] = ps.queue()
    for protocol, protocol_step in SEQUENCING:
        proto = nsc.lims.get_protocols(name=protocol)[0]
        for ps in proto.steps:
            if protocol_step == ps.name:
                queues[(protocol, protocol_step)] = ps.queue()




class ProjectStatus(object):
    """Represents the progress / status of each project"""

    def __init__(
            self, name, completed_date = None,
            step = None, status = None, total = None
            ):
        self.name = name
        self.completed_date = completed_date
        self.step = step
        self.status = status
        self.total = total

class QueueState(object):
    def __init__(self, protocol, step, num):
        self.protocol = protocol
        self.step = step
        self.num = num

class SimpleSequencerRun(object):
    def __init__(self, project):
        self.project = project



def get_queue(protocol, step):
    q = queues[(protocol, step)]
    q.get(force=True)
    if len(q.artifacts) > 0:
        return QueueState(protocol, step, len(q.artifacts))


def background_clear_monitor(completed):
    for proc in completed:
        proc.udf['Monitor'] = False
        proc.put()


def get_processes(process_name):
    procs = nsc.lims.get_processes(type=process_name, udf={'Monitor': True})
    completed = []
    result = []
    for proc in procs:
        step = Step(nsc.lims, id=proc.id)
        if step.current_state == "COMPLETED":
            completed.append(proc)
        else:
            result.append(proc)

    if completed:
        clear_task = partial(background_clear_monitor, completed)
        t = threading.Thread(target = clear_task)
        t.run()
    return result


def read_mi_next_seq(process):
    try:
        project = process.all_inputs()[0].samples[0].project.name
    except IndexError:
        return SimpleSequencerRun("")

    return SimpleSequencerRun(project)

def read_hiseq(process):
    return None


def get_sequencing(protocol, process_name, read_function):
    procs = get_processes(process_name)
    return [read_function(proc) for proc in procs]


def get_completed_projects(projects):
    return [
            ProjectInfo(pro.name, pro.close_date) for pro in projects
            ]


@app.route('/')
def get_main():
    
    seq_queues = [get_queue(*proc) for proc in SEQUENCING]
    seq = [get_sequencing(SEQUENCING[0], read_hiseq)] +\
            [get_sequencing(sp, read_mi_next_seq) for sp in SEQUENCING[1:]]

    show_date = datetime.date.today() + datetime.timedelta(days=-COMPLETED_DAYS)
    #completed_projects = lims.get_projects(
    #        open_date=show_date,
    #        udf={'Progress': 'Delivered'}
    #        )

    #pending = get_before_sequencing(active_projects)
    #sequencing = get_sequencing(active_projects)
    #data = get_data_processing()
    #completed = get_completed_projects(completed_projects)


    ## TODO: how to pass the parameters
    return render_template(
            'project-list.xhtml',
            server=nsc.lims.baseuri,
            completedays=COMPLETED_DAYS,
            seq_queues=seq_queues,
            sequencing=seq
            )

if __name__ == '__main__':
    app.debug = True
    init_application()
    app.run()

