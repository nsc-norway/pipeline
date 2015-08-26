# Prepare data for delivery. Performs various actions depending on the 
# delivery type of the project.
# - portable hard drive -> Hard-links to delivery/ dir on secondary storage
# - diagnostics -> Copies run to diagnostics area, sets permissions
# - norstore -> tars the project, computes md5 of tar, saves the tar and md5 
#               in delivery/

import sys
import os
import re
import subprocess
import crypt
from genologics.lims import *
from common import nsc, utilities, taskmgr

TASK_NAME = "90. Prepare delivery"
TASK_DESCRIPTION = """Prepare for delivery."""
TASK_ARGS = ['work_dir']

if nsc.TAG == "prod":
    from common import secure
else:
    print "Using dummy security module"
    from common import secure_dummy as secure


def delivery_diag(project_name, source_path):
    args = [nsc.RSYNC, '-rltW', '--chmod=ug+rwX,o-rwx'] # chmod 660
    args += [source_path.rstrip("/"), nsc.DIAGNOSTICS_DELIVERY]
    # (If there is trouble, see note in copyfiles.py about SELinux and rsync)
    subprocess.check_call(args)
    

def delivery_harddrive(project_name, source_path):
    subprocess.check_call(["/bin/cp", "-rl", source_path, nsc.DELIVERY_DIR])

def delivery_norstore(process, project_name, source_path):
    """Create a tar file"""

    project_dir = os.path.basename(source_path).rstrip("/")
    save_path = os.path.join(nsc.DELIVERY_DIR, project_dir)
    try:
        os.mkdir(save_path)
    except OSError:
        pass
    tarname = project_dir + ".tar"
    args = ["/bin/tar", "cf", save_path + "/" + tarname , project_dir]
    subprocess.check_call(args, cwd=os.path.dirname(source_path)) # dirname = parent dir
    #md5_path = os.path.join(nsc.DELIVERY_DIR, project_dir + "md5sum.txt")
    md5_path = os.path.join(save_path + "/md5sum.txt")
    with open(md5_path, "w") as md5file:
        # would use normal md5sum, but we have md5deep as a dependency already
        subprocess.check_call([nsc.MD5DEEP, "-l", tarname], cwd=save_path, stdout=md5file)

    # Generate username / password files
    match = re.match("^([^-]+)-(.*)-\d\d\d\d-\d\d-\d\d", project_name)
    name = match.group(1)
    proj_type = match.group(2)
    username = name.lower() + "-" + proj_type.lower()
    password = secure.get_norstore_password(process)
    crypt_pw = crypt.crypt(password)
    
    htaccess = """\
AuthUserFile /norstore_osl/projects/N59012K/www/hts-nonsecure.uio.no/{project_dir}/.htpasswd
AuthGroupFile /dev/null
AuthName ByPassword
AuthType Basic

<Limit GET>
require user {username}
</Limit>
    """.format(project_dir=project_dir, username=username)
    open(save_path + "/.htaccess", "w").write(htaccess)

    htpasswd = "{username}:{crypt_pw}\n".format(username=username, crypt_pw=crypt_pw)
    open(save_path + "/.htpasswd", "w").write(htpasswd)
    


def main(task):
    task.running()

    if not task.process:
        task.fail("Sorry, delivery prep is only available through LIMS")

    lims_projects = {}
    for i in task.process.all_inputs(unique=True):
        pro = i.samples[0].project
        lims_projects[pro.name] = pro

    runid = task.run_id
    instrument = utilities.get_instrument_by_runid(runid)

    projects = (project for project in task.projects if not project.is_undetermined)

    for project in projects:
        lims_project = lims_projects[project.name]

        project_path = os.path.join(task.bc_dir, project.proj_dir)

        delivery_type = lims_project.udf[nsc.DELIVERY_METHOD_UDF]
        project_type = lims_project.udf[nsc.PROJECT_TYPE_UDF]

        if project_type == "Diagnostics":
            task.info("Delivering " + project.name + " to diagnostics...")
            delivery_diag(project.name, project_path)
        elif delivery_type == "User HDD" or delivery_type == "New HDD":
            task.info("Copying " + project.name + " to delivery area...")
            delivery_harddrive(project.name, project_path)
        elif delivery_type == "Norstore":
            task.info("Tar'ing and copying " + project.name + " to delivery area, for Norstore...")
            delivery_norstore(task.process, project.name, project_path)
        else:
            print "No delivery prep done for project", project_name

    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

