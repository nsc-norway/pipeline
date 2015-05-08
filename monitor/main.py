from flask import Flask
from genologics.lims import *
from genologics import config
import datetime

app = Flask(__name__)


class Section(object):
    def __init__(self, name):
        self.name = name


def get_pending(active_projects):
    for p in active_projects:
        if p.TODO:
            pass


def get_sample_prep():
    pass


def get_library_prep():
    pass


def get_sequencing():
    pass

def get_data_processing():
    pass


@app.route('/')
def get_main():
    lims = Lims(config.BASEURI, config.USERNAME, config.PASSWORD)
    active_projects = lims.get_projects(udf={'Progress!': 'Delivered'})
    show_date = datetime.date.today + datetime.timedelta(days=-30)
    completed_projects = lims.get_projects(
            open_date=show_date,
            udf={'Progress': 'Delivered'}
            )

    pending = get_pending(active_projects)
    sample_prep = get_sample_prep()


    # TODO: how to pass the parameters
    return render_template('project-list.xhtml', server="ous-lims")

if __name__ == '__main__':
    app.run()
