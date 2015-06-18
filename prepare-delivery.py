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
from genologics.lims import *
from common import nsc, parse, utilities


def delivery_diag(project_name, source_path):
    args = [nsc.RSYNC, '-rltW', '--chmod=ug+rwX,o-rwx'] # chmod 660
    args += [source_path.rstrip("/"), DIAGNOSTICS_DELIVERY]
    # (If there is trouble, see note in copyfiles.py about SELinux and rsync)
    subprocess.check_call(args)
    

def delivery_harddrive(project_name, source_path):
    subprocess.check_call(["/bin/cp", "-rl", source_path, nsc.DELIVERY_DIR])

def delivery_norstore(project_name, source_path):
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
    match = re.match("^([^-])+-(.*)-\d\d\d\d-\d\d-\d\d", project_name)
    name = match.group(1)
    proj_type = match.group(2)
    username = name.lower() + "-" + proj_type.lower()
    password = TODO
    
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

    htpasswd = "{username}:{password}\n".format(username=username, password=password)
    open(save_path + "/.htpasswd", "w").write(htpasswd)
    


def main(process_id):
    os.umask(007)
    process = Process(nsc.lims, id=process_id)
    utilities.running(process)
    projects = set()
    for i in process.all_inputs(unique=True):
        projects.add(i.samples[0].project)

    if len(projects) != 1:
        print "Can only process one project at a time"
        sys.exit(1)

    project = next(iter(projects))
    seq_process = utilities.get_sequencing_process(process)
    runid = seq_process.udf['Run ID']
    instrument = utilities.get_instrument(seq_process)
    project_name = utilities.get_sample_sheet_proj_name(seq_process, project)
    if instrument == "hiseq":
        demux_path = utilities.get_demux_process(process).udf[nsc.DEST_FASTQ_DIR_UDF]
        proj_dir = parse.get_hiseq_project_dir(runid, project_name)
        project_path = os.path.join(demux_path, proj_dir)
    elif instrument == "nextseq":
        output_rundir = utilities.get_demux_process(process).udf[nsc.DEST_FASTQ_DIR_UDF]
        proj_dir = parse.get_project_dir(runid, project_name)
        project_path = os.path.join(output_rundir, proj_dir)
    elif instrument == "miseq":
        proj_dir = parse.get_project_dir(runid, project_name)
        output_rundir = os.path.join(nsc.SECONDARY_STORAGE, runid) # not the best way...
        project_path = os.path.join(output_rundir, "Data", "Intensities", "BaseCalls", proj_dir)

    delivery_type = project.udf[nsc.DELIVERY_METHOD_UDF]
    project_type = project.udf[nsc.PROJECT_TYPE_UDF]

    if project_type == "Diagnostics":
        delivery_diag(project_name, project_path)
    elif delivery_type == "User HDD" or delivery_type == "New HDD":
        delivery_harddrive(project, project_path)
    elif delivery_type == "Norstore":
        delivery_norstore(project_name, project_path)

    utilities.success_finish(process, do_finish_step=False)


with utilities.error_reporter():
    main(sys.argv[1])

