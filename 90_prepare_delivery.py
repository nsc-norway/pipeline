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
import subprocess
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
    args = [nsc.RSYNC, '-rltW', '--chmod=ug+rwX,o-rwx'] # chmod 660
    args += [project_path.rstrip("/"), nsc.DIAGNOSTICS_DELIVERY]
    # (If there is trouble, see note in copyfiles.py about SELinux and rsync)
    # Adding a generous time limit in case there is other activity going
    # on, 500 GB / 100MB/s = 1:25:00 . 
    log_path = task.logfile("rsync-" + project.name)
    rcode = remote.run_command(args, "delivery_diag", "04:00:00", storage_job=True, logfile=log_path)
    if rcode != 0:
        raise RuntimeError("Copying files to diagnostics failed, rsync returned an error")

    # Now copy quality control data

    # Diagnostics wants the QC info in a particular format (file names, etc.). Do not
    # change without consultiing with them. 

    source_qc_dir = os.path.join(basecalls_dir, "QualityControl" + task.suffix)

    dest_dir = os.path.join(
            nsc.DIAGNOSTICS_DELIVERY,
            os.path.basename(project_path)
            )
    qc_dir = os.path.join(dest_dir, "QualityControl")

    if not os.path.exists(qc_dir):
        os.mkdir(qc_dir)

    # When moving to bcl2fastq2 diag. have to decide how they extract the QC metrics.
    # For now we give them the full Reports and Stats for the whole run. Later we
    # should only give the relevant lanes.
    for subdir in ["Stats" + task.suffix, "Reports" + task.suffix]:
        source = os.path.join(basecalls_dir, subdir)
        subprocess.check_call([nsc.RSYNC, "-rlt", source, dest_dir])

    # Copy the QualityControl files
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

        for f in sample.files:
            source = os.path.join(source_qc_dir, samples.get_fastqc_dir(project, sample, f))
            fqc_name = re.sub(r".fastq.gz$", "_fastqc", f.filename)
            dest = os.path.join(sample_dir, fqc_name)
            if os.path.exists(dest):
                shutil.rmtree(dest)
            shutil.copytree(source, dest)

    # Get the demultiplex stats for diag. We generate a HTML file in the same 
    # format as that used by the first version of bcl2fastq.

    # Need to get the instrument and fcid
    instrument = utilities.get_instrument_by_runid(task.run_id)
    fcid = utilities.get_fcid_by_runid(task.run_id)
    bcl2fastq_version = utilities.get_udf(task.process, nsc.BCL2FASTQ_VERSION_UDF, None)
    undetermined_project = next(p for p in task.projects if p.is_undetermined)
    demultiplex_stats_content = demultiplex_stats.demultiplex_stats(
            project, undetermined_project, task.work_dir, basecalls_dir, instrument,
            task.no_lane_splitting, fcid, bcl2fastq_version, task.suffix
            )
    with open(os.path.join(dest_dir, "Demultiplex_Stats.htm"), 'w') as f:
        f.write(demultiplex_stats_content)


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
            args, "tar", "04:00:00",
            cwd=os.path.dirname(source_path),
            storage_job=True
            ) # dirname = parent dir
    if rcode != 0:
        raise RuntimeError('Failed to run "tar" to prepare for Norstore delivery')

    md5_path = os.path.join(save_path + "/md5sum.txt")
    # would use normal md5sum, but we have md5deep as a dependency already
    # rudimentary test indicates that md5deep only uses one thread when processing
    # a single file, so just requesting one core, and a "storage job"
    rcode = remote.run_command(
            [nsc.MD5DEEP, "-l", "-j1", tarname],
            "md5deep", "02:00:00", cwd=save_path, stdout=md5_path,
            storage_job=True
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
        password = secure.get_norstore_password(process)
        crypt_pw = crypt.crypt(password)
        
        htaccess = """\
AuthUserFile /norstore_osl/projects/NS9012K/www/hts-nonsecure.uio.no/{project_dir}/.htpasswd
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

    if not task.process:
        task.fail("Sorry, delivery prep is only available through LIMS")

    lims_projects = {}
    inputs = task.process.all_inputs(unique=True, resolve=True)
    samples = (sample for i in inputs for sample in i.samples)
    lims_projects = dict(
            (utilities.get_sample_sheet_proj_name(sample.project.name), sample.project)
            for sample in samples
            if sample.project
            )

    runid = task.run_id
    projects = (project for project in task.projects if not project.is_undetermined)

    sensitive_fail = []
    for project in projects:
        lims_project = lims_projects.get(project.name)

        if lims_project:

            project_path = os.path.join(task.bc_dir, project.proj_dir)

            delivery_type = lims_project.udf[nsc.DELIVERY_METHOD_UDF]
            project_type = lims_project.udf[nsc.PROJECT_TYPE_UDF]

            if project_type == "Diagnostics":
                task.info("Copying " + project.name + " to diagnostics...")
                delivery_diag(task, project, task.bc_dir, project_path)
            elif delivery_type == "User HDD" or delivery_type == "New HDD":
                task.info("Hard-linking " + project.name + " to delivery area...")
                delivery_harddrive(project.name, project_path)
            elif delivery_type == "Norstore":
                if project_type != "Non-Sensitive":
                    sensitive_fail.append(project.name)
                    continue
                task.info("Tar'ing and copying " + project.name + " to delivery area, for Norstore...")
                delivery_norstore(task.process, project.name, project_path)
            else:
                print "No delivery prep done for project", project_name

    if sensitive_fail:
        task.fail("Selected Norstore delivery for sensitive data, nothing done for: " + ",".join(sensitive_fail))
    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

