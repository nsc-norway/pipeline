# Prepare data for delivery. Performs various actions depending on the 
# delivery type of the project.
# - portable hard drive -> Hard-links to delivery/ dir on secondary storage
# - diagnostics -> Copies run to diagnostics area, sets permissions
# - norstore -> tars the project, computes md5 of tar, saves the tar and md5 
#               in delivery/

import sys
import os


def deliver_local():
    pass

def deliver_harddrive():
    pass

def deliver_norstore():
    pass


def main(process_id):
    projects = set()
    for i in process.all_inputs(unique=True):
        projects.add(i.samples[0].project)

    if len(projects) != 1:
        print "Project error"
        sys.exit(1)

    project = next(projects)
    delivery_type = None



main(sys.argv[1])

