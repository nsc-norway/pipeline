import os
import yaml
import sys
import shutil
import subprocess
from pathlib import Path


DIAG_DESTINATION_PATH = Path("/boston/diag/nscDelivery")
NSC_DESTINATION_PATH = Path("/data/runScratch.boston/demultiplexed")
MIK_DESTINATION_PATH = Path("/data/runScratch.boston/mik_data")

# Disable moving files. Will still copy / link files and create directory structure, but will not
# remove anything from the source folder. This can be used for testing or recovery.
TEST_DISABLE_MOVING = False


## README TROUBLESHOOTING MODE ##

# To fix issues, enable the following two "IGNORE_*" options. Remove QualityControl from all project folders in
# /boston/diag/nscDelivery and the run folder in /boston/runScratch/demultiplexed first. Fix the root cause.
# Rerun the file mover or trigger the automation again by removing the automation log.

# The following two options disable the checks and disable errors if the moving commands fail.
# This option skips over files when the source does not exist. This can be used to recover failed
# novaseq-x-file-mover runs.
IGNORE_MISSING = False
# Disable checking for existing destination files only.
IGNORE_EXISTING = False

def process_dragen_run(analysis_path):
    """Main function to process the output of an on-board DRAGEN analysis run.
    
    analysis_path: a pathlib.Path object pointing to the root of the analysis folder.
    """
    
    # Get run information
    # parents[0] "Analysis"
    # parents[1] Run folder
    input_run_dir = analysis_path.resolve().parents[1]
    run_id = input_run_dir.name
    # Analysis ID is 1, 2, ...
    analysis_id = analysis_path.name

    # Use a suffix if re-analysing the run
    if analysis_id == "1":
        analysis_suffix = ""
    else:
        analysis_suffix = f"_{analysis_id}"

    # Load LIMS-based information
    lims_file_path = analysis_dir / "ClarityLIMSImport_NSC.yaml"
    with open(lims_file_path) as f:
        lims_info = yaml.safe_load(f)

    samples = lims_info['samples']
    is_onboard_analysis = lims_info.get('compute_platform') == "Onboard DRAGEN"

    if is_onboard_analysis:
        sample['_filemover_analysis_workflow'] = sample['onboard_workflow']
    else:
        # For off-board (re)demultiplexing this is not set, and only BCL convert is available
        sample['_filemover_analysis_workflow'] = "bcl_convert"

    # Check sample info
    for sample in samples:
        if not (is_nsc(sample) or is_diag(sample) or is_mik(sample)):
            raise ValueError(f"Project {sample['project_name']} has unknown project type {sample['project_type']} "
                                f"(sample {sample['sample_name']}).")
        if not all(c.isalnum() or c in '-_' for c in sample['project_name']):
            raise ValueError(f"Project name '{repr(sample['project_name'])}' has unexpected characters and may be unsafe "
                            "to use for a file name. Please correct the yaml file.")

    # Retrieve paths and moving instruction for all fastqs and analysis files (dict indexed by project name)
    projects_info_map = get_projects_file_moving_lists(run_id, input_run_dir, samples, analysis_suffix, analysis_path, is_onboard_analysis)
    # Verify existence of source fastq and analysis files
    if not IGNORE_MISSING:
        check_missing(projects_info_map)
    # Check that project and analysis directories don't exist
    if not IGNORE_EXISTING:
        check_destinations_not_exist(projects_info_map)

    # Create run-level output folder for NSC
    if any(is_nsc(sample) for sample in samples):
        nsc_run_base = NSC_DESTINATION_PATH / run_id
        if not nsc_run_base.is_dir():
            nsc_run_base.mkdir()
        # Create analysis dir (shared for all projects)
        output_analysis_dir_path = nsc_run_base / f"Analysis{analysis_suffix}"
        if not output_analysis_dir_path.is_dir():
            output_analysis_dir_path.mkdir()
    else:
        nsc_run_base = None
    
    global_run_info_list = []
    if nsc_run_base: # Has any NSC samples -> QC info copied to NSC run base dir
        link_sav_files(input_run_dir, nsc_run_base)
        qc_dir = output_run_base / f"QualityControl{analysis_suffix}"
        qc_dir.mkdir(exist_ok=True)
        copy_app_qc_files()
        link_run_qc_files(lims_file_path, analysis_suffix, input_run_dir, analysis_dir, my_apps, output_run_base, ora, is_onboard_analysis)

    # Copy run QC files for project-based destinations (MIK, Diag)
    mik_diag_projects = set(sample['project_name'] for sample in samples if is_diag(sample) or is_mik(sample))
    for project in mik_diag_projects:
        # Create QualityControl
        qc_dir = output_run_base / f"QualityControl{analysis_suffix}"
        qc_dir.mkdir(exist_ok=True)
        project_samples = [sample for sample in samples if sample['project_name'] == project]
        output_run_base = get_dest_project_base_path(run_id, project_samples[0])
        my_apps = set(sample['_filemover_analysis_workflow'] for sample in my_samples)
        output_run_base.mkdir(exist_ok=True)
        link_sav_files(input_run_dir, output_run_base)
        link_qc_files_for_project(analysis_suffix, input_run_dir, analysis_dir, my_apps, output_run_base, ora, is_onboard_analysis)

    # Copy metrics and reports for run folder destination (NSC)
    for my_samples, output_run_base in global_run_info_list:
        my_apps = set(sample['_filemover_analysis_workflow'] for sample in my_samples)
        link_sav_files(input_run_dir, output_run_base)
        
        # Create QualityControl TODO DO ? 
        qc_dir = output_run_base / f"QualityControl{analysis_suffix}"
        qc_dir.mkdir(exist_ok=True)

        link_run_qc_files(
                lims_file_path,
                analysis_suffix,
                input_run_dir,
                analysis_dir,
                my_apps,
                output_run_base,
                my_samples[0].get('ora_compression'),
                is_onboard_analysis
                )

    # Main moving loop for project-level files - fastq and analysis
    for project_name, project_info in projects_info_map.items():
        # Create the base project directory. We have already checked that the required destinations
        # don't exist, it's okay if this one exists now.
        project_info['project_base_path'].mkdir(exist_ok=True)

        # Create destination fastq folder and move files into it.
        # For diag runs, the fastq file directory is a subdirectory of the main project directory.
        # For nsc this is the same as the base path, so we allow it to exist.
        project_info['project_fastq_path'].mkdir(exist_ok=True)
        move_files(project_info['samples_fastqs_moving'])

        # Create analysis output (we do an existence check, to allow for various
        # folder structures). Analysis is only applicable to Onboard DRAGEN.
        if lims_info.get('compute_platform') == "Onboard DRAGEN":
            if not project_info['project_analysis_path'].is_dir():
                project_info['project_analysis_path'].mkdir()
     
            move_analysis_dirs_and_files(
                project_info['project_analysis_path'],
                project_info['analysis_dirs_moving']
            )

        # Create project-specifc SampleRenamingList files for NSC projects
        project_samples = [sample for sample in samples if sample['project_name'] == project_name]
        if any(is_nsc(sample) for sample in project_samples):
            with open(nsc_run_base / f"QualityControl{analysis_suffix}" / f"SampleRenamingList-{project_name}.csv", 'w') as srl:
                srl.write(get_sample_renaming_list(project_samples, run_id, is_onboard_analysis))
 
        # Create SampleRenamingList files for all NSC projects
        nsc_samples = [sample for sample in samples if is_nsc(sample)]
        if nsc_samples:
            with open(nsc_run_base / f"QualityControl{analysis_suffix}" / f"SampleRenamingList-run.csv", 'w') as srl:
                srl.write(get_sample_renaming_list(nsc_samples, run_id, is_onboard_analysis))


def check_missing(projects_info):
    missing_fq = [
        src_file
        for project_info in projects_info.values()
        for src_file, dest_file in project_info['samples_fastqs_moving']
        if not src_file.is_file()
    ]
    if missing_fq:
        print("ERROR: Missing expected fastq file(s): " + "\n".join(str(p) for p in missing_fq))
        sys.exit(1)
    missing_analysis = [
        file_info_tuple[1][0]
        for project_info in projects_info.values()
        for file_info_tuple in project_info['analysis_dirs_moving']
        if not file_info_tuple[0].is_dir()
    ]
    if missing_analysis:
        print("ERROR: Missing expected analysis directory(s): " + "\n".join(str(p) for p in missing_analysis))
        sys.exit(1)


def check_destinations_not_exist(projects_info_map):
    for project_info in projects_info_map.values():
        if project_info['project_fastq_path'].exists():
            print(f"ERROR: Destination FASTQ dir already exists: {project_info['project_fastq_path']}")
            sys.exit(1)
        if (project_info['project_fastq_path'] != project_info['project_analysis_path']) \
                                        and project_info['project_analysis_path'].exists():
            print(f"ERROR: Destination analysis dir already exists: {project_info['project_analysis_path']}")
            sys.exit(1)


def move_files(move_file_list):
    for src, dest in move_file_list:
        if TEST_DISABLE_MOVING:
            print("mv", src, dest)
        elif IGNORE_MISSING:
            try:
                src.rename(dest)
            except IOError:
                print("Unable to move file", src, "to", dest, ", skipping.")
        else:
            src.rename(dest)


def link_sav_files(input_run_dir, output_run_base):
    # Copying in run-level files
    for run_file in ["RunParameters.xml", "RunInfo.xml"]:
        if not (output_run_base / run_file).is_file():
            os.link(input_run_dir / run_file, output_run_base / run_file)
    # Link only bin files in InterOp, without recursion into C1.1, ... dirs
    (output_run_base / "InterOp").mkdir(exist_ok=True)
    for binn in (input_run_dir / "InterOp").glob("*.bin"):
        target_path = output_run_base / "InterOp" / binn.name
        if not target_path.is_file():
            os.link(binn, target_path)


def link_run_qc_files(lims_file_path, analysis_suffix, input_run_dir, analysis_dir, my_apps, output_run_base, ora, is_onboard_analysis):
    # TODO cleanup nsc run folder copying 
    """Hard-link/Copy QC files.
        * Run level (SAV files)
        * DRAGEN analysis level (e.g. Analysis/1, as called argument of this script)
        * App level aggregate reports and logs
    """

    link_sav_files(input_run_dir, output_run_base)

    # Link the full Logs directory recursively
    if not (output_run_base / "Logs").is_dir():
        shutil.copytree(input_run_dir / "Logs", output_run_base / "Logs", copy_function=os.link)

    # Create QualityControl
    qc_dir = output_run_base / f"QualityControl{analysis_suffix}"
    qc_dir.mkdir(exist_ok=True)
    # Copy LIMS information file to output directories
    shutil.copy(lims_file_path, qc_dir)

    if is_onboard_analysis:
        # Hard-link Demux (only available for onboard analysis)
        shutil.copytree(analysis_dir / "Data" / "Demux", qc_dir / "Demux", copy_function=os.link)
    else:
        # Create equivalent demux files for off-board run
        (qc_dir / "Demux").mkdir()
        (qc_dir / "Demux" / "Demultiplex_Stats.csv").symlink_to("../BCLConvert/fastq/Reports/Demultiplex_Stats.csv")
        (qc_dir / "Demux" / "Top_Unknown_Barcodes.csv").symlink_to("../BCLConvert/fastq/Reports/Top_Unknown_Barcodes.csv")
    
    fastq_dir = "ora_fastq" if ora else "fastq"
    # Process app-level aggregated stats files
    for app in my_apps:
        app_dir = get_app_dir(app)
        (qc_dir / app_dir).mkdir()
        if is_onboard_analysis:
            shutil.copytree(
                analysis_dir / "Data" / app_dir / "AggregateReports",
                qc_dir / app_dir / "AggregateReports",
                copy_function=os.link
            )
        (qc_dir / app_dir / fastq_dir).mkdir()
        shutil.copytree(
            analysis_dir / "Data" / app_dir / fastq_dir / "Reports",
            qc_dir / app_dir / fastq_dir / "Reports",
            copy_function=os.link
        )
        # Make a copy of the used SampleSheet (real copy, not a link, just in case people get an idea
        # to edit it with vim)
        if is_onboard_analysis:
            shutil.copy(
                analysis_dir / "Data" / app_dir / "SampleSheet.csv",
                qc_dir / ("SampleSheet_" + app_dir + ".csv")
            )
            # Copy the full analysis summary to all destinations
            dest_dir = qc_dir / "summary"
            if not dest_dir.exists(): # Skip if already transferred when processing a different app
                shutil.copytree(
                    analysis_dir / "Data" / "summary",
                    qc_dir / "summary",
                    copy_function=os.link
                )
        else:
            # Alternative location for SampleSheet for redemultiplexing
            shutil.copy(
                analysis_dir / "SampleSheet.csv",
                qc_dir / ("SampleSheet_" + app_dir + ".csv")
            )


def create_filtered_demultiplex_stats(demultiplex_stats_lines, destination_path, samples):
    # TODO use this
    sample_ids = set(sample['samplesheet_sample_id'] for sample in samples)
    sample_ids.add("Undetermined")
    assert demultiplex_stats[0].split(",")[1] == "SampleID", "Demultiplex_Stats.csv correct header"
    with open(destination_path, "w") as outfile:
        outfile.write(demultiplex_stats_lines[0])
        for line in demultiplex_stats_lines[1:]:
            if line.split(",")[1] in sample_ids:
                outfile.write(line)


def copy_app_qc_files(input_analysis_dir, output_qc_dir, samples, is_onboard_analysis):
    """Copy the quality reports generated by a dragen app or off-board demultiplexing."""
    
    for sample in samples:
        apps.add(sample['_filemover_analysis_workflow'])


    # Process app-level aggregated stats files for all these samples
    fastq_dir = "ora_fastq" if ora else "fastq"
    for app in my_apps:
        app_dir = get_app_dir(app)
        (qc_dir / app_dir).mkdir()
        if is_onboard_analysis:
            shutil.copytree(
                analysis_dir / "Data" / app_dir / "AggregateReports",
                qc_dir / app_dir / "AggregateReports",
                copy_function=os.link
            )
        (qc_dir / app_dir / fastq_dir).mkdir()
        shutil.copytree(
            analysis_dir / "Data" / app_dir / fastq_dir / "Reports",
            qc_dir / app_dir / fastq_dir / "Reports",
            copy_function=os.link
        )


def move_analysis_dirs_and_files(project_analysis_target_path, sample_analysis_moving_list):
    """Moves the analysis folders of all samples in a project."""

    for src_path, (src_name, dest_name) in sample_analysis_moving_list:
        # First move the directory itself
        sample_dir = project_analysis_target_path / dest_name
        failed_moving = False
        if TEST_DISABLE_MOVING:
            print("mv", src_path, sample_dir)
        elif IGNORE_MISSING:
            try:
                src_path.rename(sample_dir)
            except IOError:
                print("Failed to move", src_path, "to", sample_dir, ", ignoreed.")
                failed_moving = True
        else:
            src_path.rename(sample_dir)

        # If the sample name should be renamed, we need to find all the files with this prefix
        # and rename them
        if src_name != dest_name and not failed_moving:
            renamable_files = list(sample_dir.glob(f"*/{src_name}.*"))
            for old_path in renamable_files:
                old_suffix = old_path.name[len(src_name):]
                new_path = old_path.parent / (dest_name + old_suffix)
                if TEST_DISABLE_MOVING:
                    print("mv", old_path, new_path)
                else:
                    old_path.rename(new_path)


def move_bcl_dirs(x):
    pass # TODO?


def get_projects_file_moving_lists(run_id, run_folder, samples, analysis_suffix, analysis_dir, is_onboard_analysis):
    """This function determines the original fastq names and the target names of where
    to move the fastq files. It also gets the corresponding info for the analysis
    directories, but the analysis also needs renaming of the contents, based on
    a pattern (not a known list of file names).

    The base path is an optional parent directory that can contain the fastq and
    analysis files for a given project (the only action is to create this path,
    for NSC it's disabled by setting it equal to fastq path).

    Output structure:
        {'PROJET_NAME': PROJECT_DATA, ...}

    PROJECT_DATA:
        {
            'project_base_path': Path,
            'project_fastq_path': Path,
            'samples_fastqs_moving': [(src_path, dest_path), (src_path, dest_path), ...],
            'project_analysis_path': Path,
            'analysis_dirs_moving': [
                (src_path, (src_sample_id, dest_sample_id)),
                (src_path, (src_sample_id, dest_sample_id)),
                ...
            ],
        }
    """

    projects = {}
    for sample in samples:
        # Determine source file set to move
        # Analysis type (_filemover_analysis_workflow): This determines the directory in which the fastqs appear (DRAGEN App dir).
        # There will also be a subdirectory for each sanmple, containing the analysis info. For BCL Convert,
        # this contains the FastQC files.
        sample_app_dir = get_app_dir(sample['_filemover_analysis_workflow'])
    
        fastq_original_paths = get_original_fastq_paths(analysis_dir, sample_app_dir, sample, is_onboard_analysis)
        fastq_destination_path = get_dest_project_fastq_dir_path(run_id, sample)
        destination_fastq_names = get_destination_fastq_names(sample, fastq_original_paths)
        destination_fastq_paths = [fastq_destination_path / dfn for dfn in destination_fastq_names]
        fastq_move_paths = [(fop, dfp) for fop, dfp in zip(fastq_original_paths, destination_fastq_paths)]

        # Analysis / QC dir for each sample.
        if is_onboard_analysis: # Only applicable for onboard DRAGEN
            if 'samplesheet_sample_project' in sample:
                sample_analysis_path = analysis_dir / "Data" / sample['samplesheet_sample_project'] / sample_app_dir / sample['samplesheet_sample_id']
            else:
                sample_analysis_path = analysis_dir / "Data" / sample_app_dir / sample['samplesheet_sample_id']
            analysis_sample_rename_tuple = (sample['samplesheet_sample_id'], get_new_sample_id(sample))
            analysis_move = set([(sample_analysis_path, analysis_sample_rename_tuple)])
        else:
            analysis_move = set()
        # Get the analysis/QC destination path for this project (not sample specific)
        analysis_destination_path = get_dest_project_analysis_dir_path(run_id, analysis_suffix, sample)

        # Save sample information for sample sheet
        project_name = sample['project_name']
        project_data = projects.get(project_name)
        if project_data is None:
            project_data = {
                'project_base_path': get_dest_project_base_path(run_id, sample),
                'project_fastq_path': fastq_destination_path,
                'samples_fastqs_moving': fastq_move_paths,
                'project_analysis_path': analysis_destination_path,
                'analysis_dirs_moving': analysis_move, # use a set - need to de-duplicate if samples are run
                                                      # on multiple lanes
            }
            projects[project_name] = project_data
        else:
            projects[project_name]['samples_fastqs_moving'] += fastq_move_paths
            projects[project_name]['analysis_dirs_moving'].update(analysis_move) # Adds zero or one analysis item

    return projects


def get_app_dir(_filemover_analysis_workflow):
    if _filemover_analysis_workflow == "bcl_convert":
        return "BCLConvert"
    else:
        # Simple pattern - maybe this will work:
        # _filemover_analysis_workflow = "germline" -> dir name DragenGermline etc
        return "Dragen" + \
                        _filemover_analysis_workflow.upper()[0] + \
                        _filemover_analysis_workflow[1:]

def is_nsc(sample):
    return sample['project_type'] in ["Sensitive", "Non-Sensitive"]

def is_diag(sample):
    return sample['project_type']  == "Diagnostics"

def is_mik(sample):
    return sample['project_type']  == "Microbiology"


def get_original_fastq_names(sample):
    compression_type = "ora" if sample.get('ora_compression') else "gz"
    fastq_original_names = ["_".join([
                        sample['samplesheet_sample_id'],
                        f"S{sample['samplesheet_position']}",
                        "L" + str(sample['lane']).zfill(3),
                        "R" + str(read_nr),
                        f"001.fastq.{compression_type}"
                    ])
                    for read_nr in range(1, (sample['num_data_read_passes'] + 1))
    ]
    if 'num_index_reads_written_as_fastq' in sample:
        fastq_original_names += ["_".join([
                            sample['samplesheet_sample_id'],
                            f"S{sample['samplesheet_position']}",
                            "L" + str(sample['lane']).zfill(3),
                            "I" + str(read_nr),
                            f"001.fastq.{compression_type}"
                        ])
                        for read_nr in range(1, (sample['num_index_reads_written_as_fastq'] + 1))
        ]
    return fastq_original_names


def get_original_fastq_paths(analysis_dir, sample_app_dir, sample, is_onboard_analysis):
    """Determines the paths of the files to move."""

    fastq_original_names = get_original_fastq_names(sample)
    fastq_dir = "ora_fastq" if sample.get('ora_compression') else "fastq"
    if 'samplesheet_sample_project' in sample:
        if is_onboard_analysis:
            # Onboard analysis with project names
            fastq_path = analysis_dir / "Data" / sample['samplesheet_sample_project'] / sample_app_dir / fastq_dir / sample['samplesheet_sample_project']
        else:
            # Redemultiplexing using novaseq-x-redemultiplexing.py script
            fastq_path = analysis_dir / "Data" / sample_app_dir / fastq_dir / sample['samplesheet_sample_project']
    else: # No Sample_Project - same for onboard and offboard
        fastq_path = analysis_dir / "Data" / sample_app_dir / fastq_dir

    return [
        fastq_path / fastq_name
        for fastq_name in fastq_original_names
    ]


def get_dest_project_base_path(run_id, sample):
    """Return a Path pointing to the target project directory for this sample."""

    if is_diag(sample):
        return DIAG_DESTINATION_PATH / get_project_dir_name(run_id, sample)
    elif is_nsc(sample): # Create intermediate directory with Run ID
        return NSC_DESTINATION_PATH / run_id / get_project_dir_name(run_id, sample)
    elif is_mik(sample):
        return MIK_DESTINATION_PATH / get_project_dir_name(run_id, sample)


def get_destination_fastq_names(sample):
    """Get the target names of the fastq files in the same order as the input names / fastq reads."""

    if is_nsc(sample):
        # Change samplesheet_sample_id to sample_name.
        compression_type = "ora" if sample.get('ora_compression') else "gz"
        output_names = [
            "_".join([
                sample['sample_name'],
                f"S{sample['samplesheet_position']}",
                "L" + str(sample['lane']).zfill(3),
                "R" + str(read)
                ]) + f"_001.fastq.{compression_type}"
            for read in range(1, sample['num_data_read_passes'] + 1)
        ]
        if 'num_index_reads_written_as_fastq' in sample:
                output_names += ["_".join([
                            sample['samplesheet_sample_id'],
                            f"S{sample['samplesheet_position']}",
                            "L" + str(sample['lane']).zfill(3),
                            "I" + str(read_nr),
                            f"001.fastq.{compression_type}"
                        ])
                        for read_nr in range(1, (sample['num_index_reads_written_as_fastq'] + 1))
            ]
        return output_names
    else:
        # Keep the original names
        return get_original_fastq_names(sample)


def get_dest_project_fastq_dir_path(run_id, sample):
    """Return a Path pointing to the directory for FASTQ files for this sample."""

    if is_diag(sample):
        return DIAG_DESTINATION_PATH / get_project_dir_name(run_id, sample) / "fastq"
    elif is_mik(sample):
        return MIK_DESTINATION_PATH / get_project_dir_name(run_id, sample) / "fastq"
    elif is_nsc(sample): # Create intermediate directory with Run ID
        return NSC_DESTINATION_PATH / run_id / get_project_dir_name(run_id, sample)
    else:
        raise ValueError("Unknown sample type")


def get_project_dir_name(run_id, sample):
    """Get project directory name (common NSC format).
    
    YYMMDD_LH00534.A.Project_Diag-wgs123-2029-01-01"""

    runid_parts = run_id.split("_")
    flowcell_side = runid_parts[-1][0] # A or B
    # Trim off the first two year digits
    date = runid_parts[0][2:]
    serial_no = runid_parts[1]
    return f"{date}_{serial_no}.{flowcell_side}.Project_{sample['project_name']}"


def get_dest_project_analysis_dir_path(run_id, analysis_suffix, sample):
    """Get the path to be created, for containing all the analysis folders for a specific project."""

    if is_diag(sample):
        # TBC placement of analysis / FastQC files?
        return DIAG_DESTINATION_PATH / get_project_dir_name(run_id, sample) / "analysis"
    elif is_nsc(sample):
        return NSC_DESTINATION_PATH / run_id / f"Analysis{analysis_suffix}" / get_project_dir_name(run_id, sample)
    elif is_mik(sample):
        return MIK_DESTINATION_PATH / get_project_dir_name(run_id, sample) / "analysis"
    else:
        raise ValueError("Unsupported project type")


def get_new_sample_id(sample):
    """Get the new sample name to use for this sample. Can be equal to the old name
    (samplesheet_sample_id)."""

    if is_diag(sample):
        return sample['samplesheet_sample_id']
    elif is_nsc(sample) or is_mik(sample):
        return sample['sample_name']
    else:
        raise ValueError("Unsupported project type")


def get_sample_renaming_list(samples, run_id, is_onboard_analysis):
    """File to convert the samplesheet sample names to the moved file names created by
    this script.
    """

    data = []
    num_fastq_files = None
    header = "Lane,OldSampleID,NewSampleID,ProjectName,NSC_ProjectName,Fastq"
    for sample in samples:
        # we have to repeat a bit of the work that's done in the moving function - to generate the
        # full fastq paths
        sample_app_dir = get_app_dir(sample['_filemover_analysis_workflow'])
        fastq_original_paths = get_original_fastq_paths(analysis_dir, sample_app_dir, sample, is_onboard_analysis)
        fastq_destination_path = get_dest_project_fastq_dir_path(run_id, sample)
        destination_fastq_names = get_destination_fastq_names(sample, fastq_original_paths)
        for fastq_name in destination_fastq_names:
            data.append(
                ",".join([
                    str(sample['lane']),
                    sample['samplesheet_sample_id'],
                    get_new_sample_id(sample),
                    sample['project_name'],
                    get_project_dir_name(run_id, sample),
                    fastq_name
                ]))
    return "\n".join([header] + data) + "\n"

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("use: novaseq-x-file-mover.py ANALYSIS_PATH")
        print()
        print("e.g. python novaseq-x-file-mover.py /data/runScratch.boston/NovaSeqX/RUN_ID/Analysis/1")
        print()
        sys.exit(1)

    analysis_dir = Path(sys.argv[1])
    if not analysis_dir.is_dir():
        print("error: analysis dir", analysis_dir, "does not exist.")
        sys.exit(1)

    process_dragen_run(analysis_dir)

