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
import glob
import demultiplex_stats
from genologics.lims import *
from common import nsc
# Small hack to use old pipeline host for 90_prepare_delivery
nsc.SBATCH_ARGLIST=["/usr/bin/sbatch", "--partition=prod", "--qos=prod"]
nsc.DIAGNOSTICS_DELIVERY = "/data/diag/nscDelivery"
from common import utilities, taskmgr, remote, samples

TASK_NAME = "91. Prepare delivery"
TASK_DESCRIPTION = """Prepare for delivery (vali)."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'lanes']

if nsc.TAG == "prod":
    from common import secure
else:
    sys.stderr.write("Using dummy security module\n")
    from common import secure_dummy as secure


def delivery_16s(task, project, lims_project, delivery_method, basecalls_dir, project_path):
    """Special delivery method for demultiplexing internal 16S barcodes."""

    assert task.process is not None, "delivery_16s can only run with LIMS mode."

    lims_param_dir = os.path.join(project_path, "lims_parameters")
    try:
        os.mkdir(lims_param_dir)
    except OSError as e:
        if e.errno == 17:
            pass
        else:
            raise e
    sample_sheet_file, sample_metadata_file, parameter_file = [
            os.path.join(lims_param_dir, filename)
            for filename in ["16SSampleSheet.tsv", "sample-metadata.tsv", "params.csv"]
            ]

    outputs = task.lims.get_batch(o['uri'] for i,o in task.process.input_output_maps
                                    if o['output-generation-type'] == "PerReagentLabel")
    with open(sample_sheet_file, "w") as f:
        for output in outputs:
            reagent = next(iter(output.reagent_labels))
            m = re.match(r"16S_... \((.*)-(.*)\)", reagent)
            if m:
                bc1, bc2 = m.groups((1,2))
            else:
                bc1 = ""
                bc2 = ""
            sample_name = re.sub(r"^\d+-", "", output.name)
            f.write("\t".join((sample_name, bc1, bc2)) + "\n")

    with open(sample_metadata_file, "w") as f:
        f.write("\t".join(["sample-id", "nsc-sample-number", "nsc-prep-batch", "nsc-row", "nsc-column"]) + "\n")
        prep_batches = dict()
        for output in outputs:
            match = re.match(r"^(\d+)-(.*)$", output.name)
            if match:
                sample_number, sample_name = match.groups()
            else:
                sample_number, sample_name = "X", output.name
            process = task.process
            label = next(iter(output.reagent_labels))
            while process and not (process.type_name.startswith("16S") and 'Sample prep' in process.type_name):
                inputs = [i['uri'] for i, o in process.input_output_maps if o['uri'].id == output.id]
                if len(inputs) == 1:
                    input = inputs[0]
                else:
                    try:
                        input = next(input for input in inputs if next(iter(input.reagent_labels)) == label)
                    except StopIteration:
                        task.warn("Unable to find ancestor artifact for {} at process {} (number of inputs: {})".format(
                            sample_name, process.id, len(inputs)))
                        process = None
                        break
                process = input.parent_process
                output = input
            if process:
                container = output.location[0]
                try:
                    i_batch = prep_batches[container.id]
                except KeyError:
                    i_batch = len(prep_batches) + 1
                    prep_batches[container.id] = i_batch
                batch, row, col = [str(i_batch)] + output.location[1].split(":")
            else:
                batch, row, col = "NA", "", ""
            f.write("\t".join([sample_name, sample_number, batch, row, col]) + "\n")


    with open(parameter_file, "w") as f:
        ps  =    [["RunID",     task.run_id]]
        ps.append(["bcl2fastq", utilities.get_bcl2fastq2_version(task.process, task.work_dir)])
        ps.append(["RTA",       utilities.get_rta_version(task.work_dir)])
        ps.append(["DeliveryMethod",delivery_method])
        ps.append(["ProjectName",project.name])
        f.write("\n".join(",".join(p) for p in ps) + "\n")

    if delivery_method == "Norstore": 
        project_dir = os.path.basename(project_path).rstrip("/")
        create_htaccess_files(task.process, project.name, project_dir, lims_param_dir)
    
    seq_process = utilities.get_sequencing_process(task.process)
    lims_info = utilities.LimsInfo(lims_project, seq_process)
    #if lims_info.total_number_of_lanes == 1 + lims_info.status_map('COMPLETED', 0):
        # All runs have been completed for this project
    subprocess.call(["/data/runScratch.boston/scripts/run-16s-pipeline.sh", project_path])


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
    rcode = remote.run_command(args, task, "delivery_diag", "04:00:00", srun_user_args=['--nodelist=vali'], logfile=log_path, comment=task.run_id)
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
            task.info("Waiting for directory {0} to appear on remote filesystem...".format(
                os.path.basename(project_path)))
            time.sleep(30) # Try again after a delay. Seems to be a timing issue related to running on different nodes.

    # When moving to bcl2fastq2 diag. have to decide how they extract the QC metrics.
    # For now we give them the full Reports and Stats for the whole run. Later we
    # should only give the relevant lanes.
    for subdir in ["Stats" + task.suffix, "Reports" + task.suffix]:
        source = os.path.join(basecalls_dir, subdir)
        subprocess.call(rsync_args + [source, dest_dir])

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

        if task.instrument in ['hiseqx', 'hiseq4k', 'novaseq']:
            source = os.path.join(source_qc_dir, samples.get_fastdup_path(project, sample, sample.files[0]))
            if os.path.exists(source):
                fdp_name = re.sub(r".fastq.gz$", "_fastdup.txt", sample.files[0].filename)
                dest = os.path.join(sample_dir, fdp_name)
                if os.path.exists(dest):
                    shutil.rmtree(dest)
                subprocess.check_call(rsync_args + [source, dest])

        for f in sample.files:
            source = os.path.join(source_qc_dir, samples.get_fastqc_dir(project, sample, f) + "/")
            if os.path.exists(source):
                fqc_name = re.sub(r".fastq.gz$", "_fastqc/", f.filename)
                dest = os.path.join(sample_dir, fqc_name)
                if os.path.exists(dest):
                    shutil.rmtree(dest)
                subprocess.check_call(rsync_args + [source, dest])

    # Get the demultiplex stats for diag. We generate a HTML file in the same 
    # format as that used by the first version of bcl2fastq.

    fcid = utilities.get_fcid_by_runid(task.run_id)
    bcl2fastq_version = utilities.get_bcl2fastq2_version(task.process, task.work_dir)
    undetermined_project = next(p for p in task.projects if p.is_undetermined)
    demultiplex_stats_content = demultiplex_stats.demultiplex_stats(
            project, undetermined_project, task.work_dir, basecalls_dir, task.instrument,
            task.no_lane_splitting, fcid, bcl2fastq_version, task.suffix
            )
    with open(os.path.join(dest_dir, "Demultiplex_Stats.htm"), 'w') as f:
        f.write(demultiplex_stats_content)

    subprocess.check_call(["/bin/chmod", "-R", "ug+rwX,o-rwx", os.path.join(nsc.DIAGNOSTICS_DELIVERY, dest_dir)])


def copy_sav_files(task, dest_dir, srun_user_args=[]):
    # Copy "SAV" files for advanced users
    if task.instrument == "nextseq":
        sav_include_paths = [
            "RunInfo.xml",
            "RunParameters.xml",
            "InterOp",
            ]
    else:
        sav_include_paths = [
            "RunInfo.xml",
            "runParameters.xml",
            "InterOp",
            ]
    demultiplexing_sample_sheets = glob.glob(os.path.join(task.work_dir, "DemultiplexingSampleSheet*.csv"))
    if demultiplexing_sample_sheets:
        sav_include_paths.append(os.path.relpath(sorted(demultiplexing_sample_sheets)[-1], task.work_dir))
    rsync_cmd = [nsc.RSYNC, '-r']
    rsync_cmd += sav_include_paths
    rsync_cmd += [os.path.join(dest_dir, task.run_id) + "/"]
    rcode = remote.run_command(rsync_cmd, task, "rsync_sav_files", time="1:00:00",
            srun_user_args=srun_user_args, cwd=task.work_dir, comment=task.run_id)
    # Rsync error code is ignored, failure here is not fatal.


def delivery_external_user(task, lims_project, project_path, delivery_path):
    """Link the fastq files, close LIMS project, and copy SAV data if specified"""

    dest_dir = os.path.join(delivery_path, os.path.basename(project_path))
    subprocess.check_call(["/bin/cp", "-rl", project_path, dest_dir])
    lims_project.close_date = datetime.date.today()
    lims_project.put()
    copy_sav_files(task, dest_dir)


def delivery_harddrive(project_name, source_path):
    # Copy to delivery area
    subprocess.check_call(["/bin/cp", "-rl", source_path, nsc.DELIVERY_DIR])
    #log_path = task.logfile("rsync-" + project_name)
    #args = [nsc.RSYNC, '-rlt', '--chmod=ug+rwX,o-rwx'] # chmod 660
    #args += [source_path.rstrip("/"), nsc.DELIVERY_DIR]
    #rcode = remote.run_command(args, task,  "delivery_hdd", "04:00:00", logfile=log_path)
    #if rcode != 0:
    #    raise RuntimeError("Copying files to loki failed, rsync returned an error")


def create_htaccess_files(process, project_name, project_dir, save_path):
    # Generate username / password files
    match = re.match("^([^-]+)-([^-]+)-\d\d\d\d-\d\d-\d\d$", project_name)
    if match:
        name = match.group(1)
        proj_type = match.group(2)
        username = name.lower() + "-" + proj_type.lower()
        password = secure.get_norstore_password(process, project_name)
    else:
        username = "invalid"
        password = "invalid"
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
            comment=task.run_id
            ) # dirname = parent dir
    if rcode != 0:
        raise RuntimeError('Failed to run "tar" to prepare for Norstore delivery')

    md5_path = os.path.join(save_path + "/md5sum.txt")
    # would use normal md5sum, but we have md5deep as a dependency already
    rcode = remote.run_command(
            nsc.MD5 + [tarname], task,
            "md5deep_tar", "08:00:00", cwd=save_path, stdout=md5_path,
            comment=task.run_id
            )
    if rcode != 0:
        raise RuntimeError("Failed to compute checksum for tar file for Norstore, "+
                "md5deep returned an error")

    try:
        create_htaccess_files(process, project_name, project_dir, save_path) 
    except Exception as e:
        task.warn("Password generation failed: " + str(e))


def main(task):
    task.running()

    lims_projects = {}
    if not os.path.isdir(nsc.DELIVERY_DIR):
        task.fail("Delivery directory {0} does not exist.".format(nsc.DELIVERY_DIR))

    if task.process:
        inputs = task.process.all_inputs(unique=True, resolve=True)
        l_samples = (sample for i in inputs for sample in i.samples)
        lims_projects = dict(
                (utilities.get_sample_sheet_proj_name(sample.project.name), sample.project)
                for sample in l_samples
                if sample.project
                )
    else:
        lims_projects = {}

    runid = task.run_id
    projects = list(project for project in task.projects if not project.is_undetermined)
    samples.add_index_read_files(projects, task.work_dir)

    sensitive_fail = []
    for project in projects:
        project_path = os.path.join(task.bc_dir, project.proj_dir)

        lims_project = lims_projects.get(project.name)
        if lims_project:
            delivery_type = lims_project.udf[nsc.DELIVERY_METHOD_UDF]
            project_type = lims_project.udf[nsc.PROJECT_TYPE_UDF]
            project_16s = lims_project.udf.get(nsc.PROJECT_16S_UDF)
        elif not task.process:
            delivery_type = nsc.DEFAULT_DELIVERY_MODE
            if delivery_type is None:
                continue
            project_type = "Non-Sensitive"
            if project.name.startswith("Diag-"):
                project_type = "Diagnostics"
            project_16s = False
        else:
            task.warn("Project " + project.name + " is missing delivery information!")
            continue

        if project_type == "Diagnostics" or delivery_type == "Transfer to diagnostics":
            task.info("Copying " + project.name + " to diagnostics...")
            delivery_diag(task, project, task.bc_dir, project_path)
        
    if sensitive_fail:
        task.fail("Selected Internet-based delivery for sensitive data, nothing done for: " + ",".join(sensitive_fail))
    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

