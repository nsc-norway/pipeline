# Prepare data for delivery. Performs various actions depending on the 
# delivery type of the project.
# - portable hard drive -> Hard-links to delivery/ dir on secondary storage
# - diagnostics -> Copies run to diagnostics area, sets permissions
# - norstore -> tars the project, computes md5 of tar, saves the tar and md5 
#               in delivery/

import sys
import os
import re
import shutil
import crypt
import time
import subprocess
import datetime
import demultiplex_stats
from genologics.lims import *
from common import nsc, utilities, taskmgr, remote, samples

TASK_NAME = "90. Prepare delivery"
TASK_DESCRIPTION = """Prepare for delivery."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'lanes']

if nsc.TAG == "prod":
    from common import secure
else:
    print "Using dummy security module"
    from common import secure_dummy as secure


def delivery_diag(task, project, basecalls_dir, project_path):
    """Special delivery method for diagnostics at OUS"""

    dest_dir = os.path.join(
            nsc.DIAGNOSTICS_DELIVERY,
            os.path.basename(project_path)
            )

    # This was changed from rsync to cp for performance reasons. cp is about 3 times as fast.
    if os.path.exists(dest_dir):
        raise RuntimeError("Destination directory '" + dest_dir + "' already exists in vali")
    args = ["/bin/cp", "-r", project_path.rstrip("/"), nsc.DIAGNOSTICS_DELIVERY]
    log_path = task.logfile("cp-" + project.name)
    rcode = remote.run_command(args, task, "delivery_diag", "04:00:00", storage_job=True, srun_user_args=['--nodelist=vali'], logfile=log_path, comment=task.run_id)
    if rcode != 0:
        raise RuntimeError("Copying files to diagnostics failed, cp returned an error")

    # Now copy quality control data
    # General args for rsync to automatically set correct permissions
    rsync_args = [nsc.RSYNC, '-rltW', '--chmod=ug+rwX,o-rwx'] # chmod 770/660

    # Diagnostics wants the QC info in a particular format (file names, etc.). Do not
    # change without consultiing with them. 
    source_qc_dir = os.path.join(basecalls_dir, "QualityControl" + task.suffix)

    qc_dir = os.path.join(dest_dir, "QualityControl")

    while not os.path.exists(qc_dir):
        try:
            os.mkdir(qc_dir)
        except OSError:
            time.sleep(30) # Try again after a delay. Seems to be a timing issue related to running on different nodes.

    # When moving to bcl2fastq2 diag. have to decide how they extract the QC metrics.
    # For now we give them the full Reports and Stats for the whole run. Later we
    # should only give the relevant lanes.
    for subdir in ["Stats" + task.suffix, "Reports" + task.suffix]:
        source = os.path.join(basecalls_dir, subdir)
        subprocess.check_call(rsync_args + [source, dest_dir])

    # Copy the QualityControl files
    copy_sav_files(task, dest_dir, ['--nodelist=vali'])

    # The locations of the fastqc directories are defined by the get_fastqc_dir() 
    # function in the samples module. These directories will then be moved to a 
    # fixed hierarchy by the following code, so the diag delivery structure is
    # decoupled from how we store it in the run folder. 
    
    # This doesn't sanitise the fastqc directory names, which depend on the fastq 
    # file names. The fastq file names and directories should be kept consistent
    # not just for diagnostics but for other users, so this shouldn't be a problem.
    for sample in project.samples:
        sample_dir = os.path.join(qc_dir, "Sample_" + sample.name)
        if not os.path.exists(sample_dir):
            os.mkdir(sample_dir)

        if instrument in ['hiseqx', 'hiseq4k']:
            source = os.path.join(source_qc_dir, samples.get_fastdup_path(project, sample, sample.files[0]))
            fdp_name = re.sub(r".fastq.gz$", "_fastdup.txt", f.filename)
            dest = os.path.join(sample_dir, fdp_name)
            if os.path.exists(dest):
                shutil.rmtree(dest)
            subprocess.check_call(rsync_args + [source, dest])

        for f in sample.files:
            source = os.path.join(source_qc_dir, samples.get_fastqc_dir(project, sample, f) + "/")
            fqc_name = re.sub(r".fastq.gz$", "_fastqc/", f.filename)
            dest = os.path.join(sample_dir, fqc_name)
            if os.path.exists(dest):
                shutil.rmtree(dest)
            subprocess.check_call(rsync_args + [source, dest])

    # Get the demultiplex stats for diag. We generate a HTML file in the same 
    # format as that used by the first version of bcl2fastq.

    # Need to get the instrument and fcid
    instrument = utilities.get_instrument_by_runid(task.run_id)
    fcid = utilities.get_fcid_by_runid(task.run_id)
    if task.process:
        bcl2fastq_version = utilities.get_udf(task.process, nsc.BCL2FASTQ_VERSION_UDF, None)
    else:
        bcl2fastq_version = utilities.get_bcl2fastq2_version(task.work_dir)
    undetermined_project = next(p for p in task.projects if p.is_undetermined)
    demultiplex_stats_content = demultiplex_stats.demultiplex_stats(
            project, undetermined_project, task.work_dir, basecalls_dir, instrument,
            task.no_lane_splitting, fcid, bcl2fastq_version, task.suffix
            )
    with open(os.path.join(dest_dir, "Demultiplex_Stats.htm"), 'w') as f:
        f.write(demultiplex_stats_content)

    subprocess.check_call(["/bin/chmod", "-R", "ug+rwX,o-rwx", os.path.join(nsc.DIAGNOSTICS_DELIVERY, dest_dir)])


def copy_sav_files(task, dest_dir, srun_user_args=[]):
    # Copy "SAV" files for advanced users
    if task.instrument == "nextseq":
        SAV_INCLUDE_PATHS = [
            "RunInfo.xml",
            "RunParameters.xml",
            "InterOp",
            ]
    else:
        SAV_INCLUDE_PATHS = [
            "RunInfo.xml",
            "runParameters.xml",
            "InterOp",
            ]
    rsync_cmd = [nsc.RSYNC, '-r']
    rsync_cmd += SAV_INCLUDE_PATHS
    rsync_cmd += [os.path.join(dest_dir, task.run_id) + "/"]
    rcode = remote.run_command(rsync_cmd, task, "rsync_sav_files", time="01:00", storage_job=True,
            srun_user_args=srun_user_args, cwd=task.work_dir, comment=task.run_id)
    # Rsync error code is ignored, failure here is not fatal.


def delivery_mik(task, lims_project, project_path):
    dest_dir = os.path.join("/data/runScratch.boston/mik_data", os.path.basename(project_path))
    subprocess.check_call(["/bin/cp", "-rl", project_path, dest_dir])
    lims_project.close_date = datetime.date.today()
    lims_project.put()
    copy_sav_files(task, dest_dir)


def delivery_imm(task, lims_project, project_path):
    dest_dir = os.path.join("/data/runScratch.boston/imm_data", os.path.basename(project_path))
    subprocess.check_call(["/bin/cp", "-rl", project_path, dest_dir])
    lims_project.close_date = datetime.date.today()
    lims_project.put()
    #copy_sav_files(task, dest_dir)


def delivery_harddrive(project_name, source_path):
    # Copy to delivery area
    subprocess.check_call(["/bin/cp", "-rl", source_path, nsc.DELIVERY_DIR])
    #log_path = task.logfile("rsync-" + project_name)
    #args = [nsc.RSYNC, '-rlt', '--chmod=ug+rwX,o-rwx'] # chmod 660
    #args += [source_path.rstrip("/"), nsc.DELIVERY_DIR]
    #rcode = remote.run_command(args, task,  "delivery_hdd", "04:00:00", storage_job=True, logfile=log_path)
    #if rcode != 0:
    #    raise RuntimeError("Copying files to loki failed, rsync returned an error")


def delivery_norstore(process, project_name, source_path, task):
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
            args, task, "tar", "04:00:00",
            cwd=os.path.dirname(source_path),
            storage_job=True, comment=task.run_id
            ) # dirname = parent dir
    if rcode != 0:
        raise RuntimeError('Failed to run "tar" to prepare for Norstore delivery')

    md5_path = os.path.join(save_path + "/md5sum.txt")
    # would use normal md5sum, but we have md5deep as a dependency already
    # rudimentary test indicates that md5deep only uses one thread when processing
    # a single file, so just requesting one core, and a "storage job"
    rcode = remote.run_command(
            [nsc.MD5DEEP, "-l", "-j1", tarname], task,
            "md5deep_tar", "08:00:00", cwd=save_path, stdout=md5_path,
            storage_job=True, comment=task.run_id
            )
    if rcode != 0:
        raise RuntimeError("Failed to compute checksum for tar file for Norstore, "+
                "md5deep returned an error")

    # Generate username / password files
    try:
        match = re.match("^([^-]+)-([^-]+)-\d\d\d\d-\d\d-\d\d$", project_name)
        name = match.group(1)
        proj_type = match.group(2)
        username = name.lower() + "-" + proj_type.lower()
        password = secure.get_norstore_password(process, project_name)
        crypt_pw = crypt.crypt(password)
        
        htaccess = """\
AuthUserFile /data/{project_dir}/.htpasswd
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
    except Exception, e:
        task.warn("Password generation failed: " + str(e))
    


def main(task):
    task.running()

    lims_projects = {}
    if task.process:
        inputs = task.process.all_inputs(unique=True, resolve=True)
        samples = (sample for i in inputs for sample in i.samples)
        lims_projects = dict(
                (utilities.get_sample_sheet_proj_name(sample.project.name), sample.project)
                for sample in samples
                if sample.project
                )
    else:
        lims_projects = {}

    runid = task.run_id
    projects = (project for project in task.projects if not project.is_undetermined)

    sensitive_fail = []
    for project in projects:
        project_path = os.path.join(task.bc_dir, project.proj_dir)

        lims_project = lims_projects.get(project.name)
        if lims_project:
            delivery_type = lims_project.udf[nsc.DELIVERY_METHOD_UDF]
            project_type = lims_project.udf[nsc.PROJECT_TYPE_UDF]
        elif not task.process:
            delivery_type = nsc.DEFAULT_DELIVERY_MODE
            if delivery_type is None:
                continue
            project_type = "Non-Sensitive"
            if project.name.startswith("Diag-"):
                project_type = "Diagnostics"
        else:
            task.warn("Project " + project.name + " is missing delivery information!")
            continue

        if project_type == "Diagnostics":
            task.info("Copying " + project.name + " to diagnostics...")
            delivery_diag(task, project, task.bc_dir, project_path)
        elif project_type == "Immunology":
            delivery_imm(task, lims_project, project_path)
        elif project_type == "Microbiology":
            delivery_mik(task, lims_project, project_path)
        elif delivery_type in ["User HDD", "New HDD", "NeLS project", "TSD project"]:
            task.info("Hard-linking " + project.name + " to delivery area...")
            delivery_harddrive(project.name, project_path)
        elif delivery_type == "Norstore":
            if project_type != "Non-Sensitive":
                sensitive_fail.append(project.name)
                continue
            task.info("Tar'ing and copying " + project.name + " to delivery area, for Norstore...")
            delivery_norstore(task.process, project.name, project_path, task)
        else:
            print "No delivery prep done for project", project.name

    if sensitive_fail:
        task.fail("Selected Norstore delivery for sensitive data, nothing done for: " + ",".join(sensitive_fail))
    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

