from flask import Flask, render_template
from genologics.lims import *
import re
import requests
from common import nsc, utilities
import datetime
import threading
from functools import partial
from collections import defaultdict

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
app.debug=True

INSTRUMENTS = ["HiSeq", "NextSeq", "MiSeq"]
# [ (Protocol, Step) ]
SEQUENCING = [
        ("Illumina SBS (HiSeq GAIIx) 5.0", "Illumina Sequencing (Illumina SBS) 5.0"),
        ("Illumina SBS (NextSeq) 1.0", "NextSeq Run (NextSeq) 1.0"),
        ("Illumina SBS (MiSeq) 5.0", "MiSeq Run (MiSeq) 5.0")
        ]

# [ (Protocol, (Step, Step, Step)) ]
DATA_PROCESSING = [
        ("NSC Data processing for HiSeq", (
            "NSC Demultiplexing (HiSeq)",
            "NSC Data Quality Reporting (HiSeq)",
            "NSC Prepare for delivery",
            "NSC Finalize run"
            )),
        ("NSC Data processing for NextSeq", (
            "NSC Demultiplexing (NextSeq)",
            "NSC Data Quality Reporting (Mi/NextSeq)",
            "NSC Prepare for delivery",
            "NSC Finalize run"
            )),
        ("NSC Data processing for MiSeq", (
            "NSC Copy MiSeq Run",
            "NSC Data Quality Reporting (Mi/NextSeq)",
            "NSC Prepare for delivery",
            "NSC Finalize run"
            )),
        ]


queues = {}
sequencing_process_type = []

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
                sequencing_process_type.append(ps.process_type)


class QueueState(object):
    def __init__(self, url, protocol, step, num):
        self.url = url
        self.protocol = protocol
        self.step = step
        self.num = num
        self.is_queue = True


class Project(object):
    def __init__(self, url, name):
        self.url = url
        self.name = name


class ProcessInfo(object):
    def __init__(self, name, url, flowcell_id, projects, status, seq_url, runid, finished=None):
        self.name = name
        self.url = url
        self.flowcell_id = flowcell_id
        self.projects = projects
        self.status = status
        self.seq_url = seq_url
        self.runid = runid
        self.finished = finished
        self.is_queue = False


def get_queue(protocol, step):
    """Get a single QueueState object if there are some samples in the queue,
    else returns None.

    Assumes that refresh is already done (done by batch request)"""

    q = queues[(protocol, step)]
    if len(q.artifacts) > 0:
        url = "{0}clarity/queue/{1}".format(ui_server, q.id)
        return QueueState(url, protocol, step, len(q.artifacts))
    else:
        return None


def background_clear_monitor(completed):
    for proc in completed:
        print "Disabling monitoring for", proc.id
        proc.udf['Monitor'] = False
        proc.put()


def is_step_completed(proc, step):
    """Check if the state of this Step is completed.

    Does not refresh the step, this should be done by a batch request
    prior to calling is_step_completed.
    """
    try:
        return step.current_state.upper() == "COMPLETED"
    except requests.exceptions.HTTPError:
        # If the process has no associated step, skip it
        print "No step for", step.id
	return False


def is_sequencing_complete(proc, step):
    try:
        return proc.udf['Finish Date'] != None and is_step_completed(proc, step)
    except KeyError:
        return False


def get_processes(process_name, complete_condition=is_step_completed):
    procs = nsc.lims.get_processes(type=process_name, udf={'Monitor': True})
    nsc.lims.get_batch(procs)
    steps = [Step(nsc.lims, id=proc.id) for proc in procs]
    nsc.lims.get_batch(steps)
    completed = []
    result = []
    for proc, step in zip(procs, steps):
        if complete_condition(proc, step):
            completed.append(proc)
        else:
            result.append(proc)

    if completed:
        clear_task = partial(background_clear_monitor, completed)
        t = threading.Thread(target = clear_task)
        t.run()

    return result


def proc_url(process_id):
    global ui_server
    second_part_limsid = re.match(r"[\d]+-([\d]+)$", process_id).group(1)
    return "{0}clarity/work-details/{1}".format(ui_server, second_part_limsid)


def read_project(lims_project):
    url = "{0}clarity/search?scope=Project&query={1}".format(ui_server, lims_project.id)
    return Project(url, lims_project.name)


def read_mi_next_seq(process):
    try:
        project = read_project(process.all_inputs()[0].samples[0].project)
    except IndexError:
        return SimpleSequencerRun("")
    return SimpleSequencerRun(proc_url(process.id), project)


def get_projects(process):
    lims_projects = set(
            art.samples[0].project
            for art in process.all_inputs()
            )
    return [read_project(p) for p in lims_projects]


def read_sequencing(process_name, process):
    url = proc_url(process.id)
    flowcell_id = process.all_inputs()[0].location[0].name
    if "NextSeq" in process_name:
        step = Step(nsc.lims, id=process.id)
        for lot in step.reagent_lots:
            if lot.reagent_kit.name == "NextSeq 500 FC v1":
                flowcell_id = lot.name
    if "MiSeq" in process_name:
        pass
    lims_projects = set(
            art.samples[0].project
            for art in process.all_inputs()
            )
    projects = get_projects(process)
    try:
        runid = process.udf['Run ID']
    except KeyError:
        runid = ""
    try:
        status = process.udf['Status']
    except KeyError:
        status = "Pending/running"
    try:
        finished = process.udf['Finish Date']
    except KeyError:
        finished = ""

    return ProcessInfo(
            process_name, url, flowcell_id, projects, status, url, runid, finished
            )


def get_sequencing(process_name):
    procs = get_processes(process_name, is_sequencing_complete)
    return 


def read_post_sequencing_process(process_name, process, sequencing_process):
    url = proc_url(process.id)
    seq_url = proc_url(sequencing_process.id)
    #flowcell_id = process.all_inputs()[0].location[0].name
    try:
        runid = sequencing_process.udf['Run ID']
    except (KeyError, TypeError):
        runid = ""
        expt_name = ""
    projects = get_projects(process)

    try:
        status = process.udf['Job status']
    except KeyError:
        status = "Open"


    return ProcessInfo(
            process_name, url, None, projects, status, seq_url, runid
            )



def get_recent_run(fc):
    
    


def get_recently_completed_runs():
    # Look for any flowcells which have a value for this udf
    FLOWCELL_TYPES = [
                "Illumina Flow Cell",
                "NextSeq Reagent Cartridge", 
                "MiSeq Reagent Cartridge"
                ]
    flowcells = nsc.lims.get_containers(
            udf={nsc.RECENTLY_COMPLETED_UDF: True},
            type=FLOWCELL_TYPES
            )


    cutoff_date = datetime.date.today() - datetime.timedelta(days=30)
    results = [[],[],[]]
    for fc in flowcells:
        try:
            date_str = fc.udf[nsc.PROCESSED_DATE_UDF]
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        except (KeyError, ValueError):
            date = None

        if date < cutoff_date:
            fc.udf[nsc.RECENTLY_COMPLETED_UDF] = False
            fc.put()
        else:
            # Container types will be cached, so the extra entity request is
            # not a problem
            instrument_index = FLOWCELL_TYPES.index(fc.type.name)
            sequencing_process = nsc.lims.get_processes(
                    type=SEQUENCING[instrument_index][1],
                    inputartifactlimsid=next(fc.placements)
                    )
            results[instrument_index].append(get_recent_run(fc))
        
    return results





@app.route('/')
def get_main():
    global ui_server

    ui_servers = {
            "http://dev-lims.ous.nsc.local:8080/": "https://dev-lims.ous.nsc.local/",
            "http://ous-lims.ous.nsc.local:8080/": "https://ous-lims.ous.nsc.local/"
            }
    try:
        ui_server = ui_servers[nsc.lims.baseuri]
    except KeyError:
        ui_server = nsc.lims.baseuri

    # Refresh all queues
    nsc.lims.get_batch(q for q in queues)

    seq_queues = [get_queue(*proc) for proc in SEQUENCING]

    all_process_types = [i[1] for i in SEQUENCING] +\
                [procname for wfname, proclist in DATA_PROCESSING for procname in proclist]

    # Get a list of all processes
    monitored_process_list = nsc.lims.get_processes(udf={'Monitor': True], type=all_proces_types)
    # Refresh data for all processes (need this for almost all monitored procs, so
    # doing a batch request)
    processes_with_data = nsc.lims.get_batch(monitored_process_list)
    # Need Steps to see if COMPLETED, this loads them into cache
    nsc.lims.get_batch(Step(nsc.lims, id=p.id) for p in processes_with_data)

    seq_processes = defaultdict(list)
    post_processes = defaultdict(list)
    for p in processes_with_data:
        if p.type.name in seq_procs:
            if is_sequencing_completed(p):
                seq_processes[p.type.name].append(p)
       else:
           if is_process_completed(p):
               post_processes[p.type.name]



    # List of three elements -- Hi,Next,MiSeq, each contains a list of 
    # sequencing processes
    sequencing = [
        [read_sequencing(sp[1], proc) for proc in processes[sp[1]]]
        for sp in SEQUENCING]


    # List of three sequencer types (containing lists within them)
    post_sequencing = []
    # One workflow for each sequencer type
    for index, (wf, step_names) in enumerate(DATA_PROCESSING):
        machine_items = [] # all processes, queues, for a type of sequencing machine
        for step_name in step_names:
            q = get_queue(wf, step_name)
            if q:
                machine_items.append(q)
            for process in processes[step_name]:
                sequencing_process = utilities.get_sequencing_process(process)
                if sequencing_process.type == sequencing_process_type[index]:
                    machine_items.append(read_post_sequencing_process(
                        step_name, process, sequencing_process
                        ))
        post_sequencing.append(machine_items)
        

    body = render_template(
            'processes.xhtml',
            server=nsc.lims.baseuri,
            sequencing=zip(seq_queues, sequencing),
            post_sequencing=post_sequencing,
            instruments=INSTRUMENTS
            )
    return (body, 200, {'Refresh': '300'})

init_application()
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001)

