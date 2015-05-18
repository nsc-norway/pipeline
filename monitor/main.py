from flask import Flask, render_template
from genologics.lims import *
import nsc
import datetime

# Project / Sample progress tagging
# ---------------------------------
# The Progress UDF of the Sample is set by Clarity LIMS on various stages.


# Different samples can be in different workflows
# Project 
# |- Sample 1 -> W1
# |- Sample 2 -> W1, W2
# \- Sample 3 -> W2

# Project 
# |- Sample 1
# |- Sample 2
# |          |- Artifact 1 in workflow 1
# |          |- Artifact 2 in workflow 2
# |          \_ Artifact 3 in workflow 1
# \_ Sample 3

# When a (submitted) sample is added to a workflow, an artifact for that sample is 
# automatically generated. Each artifact can have derived samples for them, which
# can be the objects taken further into the workflow. E.g. one stops working on 
# the original artifact and starts working on a pool. Derived samples can also be
# added to workflows (TODO: figure out exactly what happens).

# 1) How to define the progress of a project:
#    The project may have multiple progresses. Each artifact in a queue or in use
#    in a step (ice bucket) should be represented as a progress level. A completed
#    workflow should also be considered as the progress.
# 2) Special case: With respect to the Project Evaluation workflow, the project is
#    either evaluated or pending. If any other work has been done on the project, 
#    the evaluated or pending state is no longer relevant.

# Algorithm for determining the progress:
# 



app = Flask(__name__)


class ProjectInfo(object):
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


def get_sequencing():
    for instr, process in nsc.SEQ_PROCESSES:
        ps = nsc.lims.get_processes(type=process)

def get_data_processing():
    pass

def get_delivery():
    pass

def get_completed_projects(projects):
    return [
            ProjectInfo(pro.name, pro.close_date) for pro in projects
            ]


@app.route('/')
def get_main():
    #active_projects = lims.get_projects(udf={'Progress': 'Delivered'})
    show_date = datetime.date.today() + datetime.timedelta(days=-30)
    #completed_projects = lims.get_projects(
    #        open_date=show_date,
    #        udf={'Progress': 'Delivered'}
    #        )

    #pending = get_before_sequencing(active_projects)
    #sequencing = get_sequencing(active_projects)
    #data = get_data_processing()
    #completed = get_completed_projects(completed_projects)


    ## TODO: how to pass the parameters
    return render_template('project-list.xhtml', server="ous-lims")

if __name__ == '__main__':
    app.debug = True
    app.run()

