from flask import Flask, render_template
from genologics.lims import *
import nsc
import datetime

# Project / Sample progress
# ---------------------------------

# Method for generating the progress overview:
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

