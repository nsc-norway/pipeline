# Prepare data for delivery. Performs various actions depending on the 
# delivery type of the project.
# - portable hard drive -> Hard-links to delivery/ dir on secondary storage
# - diagnostics -> Copies run to diagnostics area, sets permissions
# - norstore -> tars the project, computes md5 of tar, saves the tar and md5 
#               in delivery/

import sys
import os
import re
import crypt
import subprocess
from genologics.lims import *
from common import nsc, utilities, taskmgr, remote

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
    # Adding a generous time limit in case there is other activity going
    # on, 500 GB / 100MB/s = 1:25:00 . 
    rcode = remote.run_command(args, "delivery_diag", "04:00:00", storage_job=True)
    if rcode != 0:
        raise RuntimeError("Copying files to diagnostics failed, rsync returned an error")


def delivery_harddrive(project_name, source_path):
    # There's no need to consider remote execution, creating hard-links is
    # done within an instant
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
    rcode = remote.run_command(
            args, "tar", "02:00:00",
            cwd=os.path.dirname(source_path),
            storage_job=True
            ) # dirname = parent dir
    if rcode != 0:
        raise RuntimeError('Failed to run "tar" to prepare for Norstore delivery')

    md5_path = os.path.join(save_path + "/md5sum.txt")
    with open(md5_path, "w") as md5file:
        # would use normal md5sum, but we have md5deep as a dependency already
        # rudimentary test indicates that md5deep only uses one thread when processing
        # a single file, so just requesting one core, and a "storage job"
        rcode = remote.run_command(
                [nsc.MD5DEEP, "-l", "-j1", tarname],
                "md5deep", "02:00:00", cwd=save_path, stdout=md5file,
                storage_job=True
                )
        if rcode != 0:
            raise RuntimeError("Failed to compute checksum for tar file for Norstore, "+
                    "md5deep returned an error")

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

