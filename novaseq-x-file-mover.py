import sys
import os
import logging
import shutil
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# Test mode disables the calls to move files
TEST_MODE = os.environ.get("TEST_MODE", "False").lower() in ("1", "true", "yes")

if TEST_MODE:
    # Test paths
    DEST_PATHS = {
        'Diagnostics': Path('test/diag'),
        'Sensitive': Path('test/nsc'),
        'Non-Sensitive': Path('test/nsc'),
        'Microbiology': Path('test/mik'),
        'PGT': Path('test/diag')
    }
    logger.info("Using test paths")
    # Ensure test paths exist   
    for create_dest_path in DEST_PATHS.values():
        create_dest_path.mkdir(exist_ok=True, parents=True)
else:
    # Production destination paths by project
    DEST_PATHS = {
        'Diagnostics': Path('/boston/diag/nscDelivery'),
        'Sensitive': Path('/data/runScratch.boston/demultiplexed'),
        'Non-Sensitive': Path('/data/runScratch.boston/demultiplexed'),
        'Microbiology': Path('/data/runScratch.boston/mik_data'),
        'PGT': Path('/boston/diag/nscDelivery')
    }

# Control flags (for testing / recovery)
# Disable moving files. Will still copy / link files and create directory structure, but will not
# remove anything from the source folder. This can be used for testing or recovery.
TEST_DISABLE_MOVING = os.environ.get("TEST_DISABLE_MOVING", "False").lower() in ("1", "true", "yes")

## READNE TROUBLESHOOTING MODE ##

# To fix issues, enable the following two "IGNORE_*" options. Remove QualityControl from all project folders in
# /boston/diag/nscDelivery and the run folder in /boston/runScratch/demultiplexed first. Fix the root cause.
# Rerun the file mover or trigger the automation again by removing the automation log.

# The following two options disable the checks and disable errors if the moving commands fail.
# This option skips over files when the source does not exist. This can be used to recover failed
# novaseq-x-file-mover runs.
IGNORE_MISSING = os.environ.get("IGNORE_MISSING", "False").lower() in ("1", "true", "yes")
# Continue even if the destination files exist.
IGNORE_EXISTING = os.environ.get("IGNORE_EXISTING", "False").lower() in ("1", "true", "yes")

@dataclass
class Project:
    name: str
    samplesheet_sample_project: str
    project_type: str
    # Contains a copy of the sample-level setting for compression
    ora_compression: bool
    # The following data members are set by the setup_paths function
    fastq_path: Path = None
    analysis_path: Path = None
    qc_path: Path = None
    run_qc_path: Path = None

    def setup_paths(self, dest_paths: Dict[str, Path], run_id, analysis_suffix):
        if self.project_type not in dest_paths:
            raise ValueError(f"Invalid project type {self.project_type} for project {self.name}")
        projecttype_root = dest_paths.get(self.project_type)
        
        if self.is_nsc():
            run_dir = projecttype_root / run_id
            # NSC: group under run, fastq and analysis separate
            sfx = '_ora' if self.ora_compression else ''
            self.fastq_path =  run_dir / (self._dir_name(run_id) + sfx)
            self.analysis_path = run_dir / f'Analysis{analysis_suffix}' / self._dir_name(run_id)
            self.qc_path = run_dir / f'QualityControl{analysis_suffix}' / self._dir_name(run_id)
            self.run_qc_path = run_dir
        elif self.is_mik():
            project_root_path =  projecttype_root / self._dir_name(run_id)
            self.fastq_path = project_root_path
            self.analysis_path = project_root_path # Analysis is not used for MIK
            self.qc_path = project_root_path / 'QualityControl'
            self.run_qc_path = project_root_path
        elif self.project_type == 'Diagnostics':
            # Diag: project root, with subdirs
            project_root_path =  projecttype_root / self._dir_name(run_id)
            self.fastq_path = project_root_path / 'fastq'
            self.analysis_path = project_root_path / 'analysis'
            self.qc_path = project_root_path / 'QualityControl'
            self.run_qc_path = project_root_path
        elif self.project_type == 'PGT':
            # Diag: project root, with subdirs
            project_root_path =  projecttype_root / self._dir_name(run_id)
            self.fastq_path = project_root_path / 'fastq'
            self.analysis_path = project_root_path / 'analysis'
            self.qc_path = project_root_path / 'QualityControl'
            self.run_qc_path = project_root_path
        else:
            raise RuntimeError(f"Unable to handle project type {self.project_type}")


    def _dir_name(self, run_id) -> str:
        runid_parts = run_id.split('_')
        date6 = runid_parts[0][2:]
        serial = runid_parts[1]
        side = runid_parts[-1][0]
        return f"{date6}_{serial}.{side}.Project_{self.name}"

    def is_nsc(self) -> bool:
        return self.project_type in ['Sensitive', 'Non-Sensitive']

    def is_mik(self) -> bool:
        return self.project_type == 'Microbiology'
    
    def enable_rename_to_sample_name(self) -> bool:
        return self.is_nsc() or self.is_mik()
    

@dataclass
class Sample:
    """Sample x lane information object
    
    Corresponds to an entry in the LIMS file, which is also equivalent to an output fastq file set
    (R1, optionally R2, I1, I2). If a sample is run on multiple lanes, there will be one entry per lane.
    If the same sample ID is used for multiple indexes in the same lane, there will just be one entry
    for all of them (BCL Convert merges them to one fastq set).
    """

    project: Project
    sample_name: str
    samplesheet_sample_id: str
    samplesheet_position: int
    lane: int
    num_data_reads: int
    num_index_reads_written_as_fastq: int = 0
    ora_compression: bool = False
    filemover_workflow: str = 'bcl_convert'

    def new_sample_id(self) -> str:
        return self.sample_name if self.project.enable_rename_to_sample_name() else self.samplesheet_sample_id

    def app_dir(self) -> str:
        if self.filemover_workflow == 'bcl_convert':
            return 'BCLConvert'
        suffix = self.filemover_workflow.capitalize()
        return f"Dragen{suffix}"


class FileMover:
    """Main class that does the work of this script
    
    Contains the processing state and analysis level (global) details."""

    def __init__(
        self,
        analysis_dir: Path,
        dest_paths: Dict[str, Path],
        test_mode: bool = False,
        ignore_missing: bool = False,
        ignore_existing: bool = False
    ):
        """
        analysis_dir: root of the DRAGEN analysis folder (e.g. RUN_ID/Analysis/1)
        nsc_root, diag_root, mik_root: base destination directories for each project type
        test_mode: if True, just print moves without performing them
        ignore_missing: skip missing source errors
        ignore_existing: skip existing destination errors
        """
        self.analysis_dir = analysis_dir.resolve()
        self.dest_paths = dest_paths
        self.nsc_base_path = dest_paths['Sensitive']
        self.test_mode = test_mode
        self.ignore_missing = ignore_missing
        self.ignore_existing = ignore_existing
        
        # derive run_id and analysis suffix
        self.run_dir = self.analysis_dir.parents[1]
        self.run_id = self.run_dir.name
        analysis_id = self.analysis_dir.name
        self.analysis_suffix = '' if analysis_id == '1' else f"_{analysis_id}"

        # load LIMS file
        self.lims_path  = self.analysis_dir / 'ClarityLIMSImport_NSC.yaml'

        # Set class members to be updated by load_lims()
        self.samples: List[Sample] = []
        self.projects: Dict[str, Project] = {}
        self.is_onboard = False
        self.compute_platform = None
        # Group projects


    def load_lims_file(self):
        logger.info(f"Loading LIMS info from {self.lims_path}")
        with open(self.lims_path) as f:
            data = yaml.safe_load(f)
        self.compute_platform = data.get('compute_platform', '')
        self.is_onboard = self.compute_platform == 'Onboard DRAGEN'
        for entry in data['samples']:
            project_name = entry['project_name']
            if project_name not in self.projects:
                self.projects[project_name] = self._create_project(entry)
            
            project = self.projects[entry['project_name']]
            assert project.project_type == entry['project_type'], "Project type should be the same for all samples in the project"
            assert project.ora_compression == entry['ora_compression'], "Compression type should be the same for all samples in the project"

            s = Sample(
                project=project,
                sample_name=entry['sample_name'],
                samplesheet_sample_id=entry['samplesheet_sample_id'],
                samplesheet_position=int(entry['samplesheet_position']),
                lane=int(entry['lane']),
                num_data_reads=int(entry['num_data_read_passes']),
                num_index_reads_written_as_fastq=int(entry.get('num_index_reads_written_as_fastq', 0)),
                ora_compression=bool(entry.get('ora_compression', False)),
                filemover_workflow=entry.get('onboard_workflow') if self.is_onboard else 'bcl_convert'
            )
            self.samples.append(s)

        logger.info(f"Loaded {len(self.samples)} samples in {len(self.projects)} projects from LIMS file")


    def _create_project(self, entry):
        if not all(c.isalnum() or c in '-_' for c in entry['project_name']):
            raise ValueError(f"Project name should only contain alphanumerics, - and _. Error: '{entry['project_name']}'")
        
        project = Project(
                            name=entry['project_name'],
                            samplesheet_sample_project=entry.get('samplesheet_sample_project'),
                            project_type=entry['project_type'],
                            ora_compression=entry['ora_compression']
                        )
        project.setup_paths(self.dest_paths, self.run_id, self.analysis_suffix)
        return project


    def check_sources_and_destinations(self):
        """Verify that the source files exist and the destination directories don't"""

        missing = []
        conflicts = []
        # check sources
        for s in self.samples:
            # Check fastq paths
            for src in self._original_fastq_paths(s):
                if not src.exists():
                    missing.append(src)
            # Check sample-level analysis folders
            src_dir = self.analysis_dir / 'Data' / s.app_dir() / s.samplesheet_sample_id
            if not src_dir.exists():
                missing.append(src_dir)
        # check destinations
        for s in self.samples:
            for name in self._dest_fastq_names(s):
                dest = s.project.fastq_path / name
                if dest.exists():
                    conflicts.append(dest)
        for p in self.projects.values():
            if p.analysis_path.exists():
                conflicts.append(p.analysis_path)
        if missing and not IGNORE_MISSING:
            for m in missing: logger.error(f"Missing source: {m}")
            sys.exit(1)
        if conflicts and not IGNORE_EXISTING:
            for c in conflicts: logger.error(f"Destination already exists: {c}")
            sys.exit(1)
        logger.info("Source & destination checks passed")


    def prepare_directories(self):
        """Create the destination directories for project and / or run"""

        # project-level (implicitly creates NSC run dir if required)
        for p in self.projects.values():
            for path in (p.fastq_path, p.analysis_path, p.qc_path):
                path.mkdir(parents=True, exist_ok=True)
        logger.info("Directory structure created")


    def move_sample_files(self):
        """Move all fastq and analysis files related to all samples"""

        for s in self.samples:
            logger.info(f"Processing sample {s.sample_name}")
            self._move_fastqs(s)
            if self.is_onboard:
                # Onboard DRAGEN: Creates an analysis folder for each sample, even if only BCL Convert, it creates FastQC
                if not s.project.is_mik(): # Analysis outputs not included for MIK, to simplify folder
                    self._move_analysis(s)
            

    def _move_fastqs(self, s: Sample):
        for from_path, to_name in zip(self._original_fastq_paths(s), self._dest_fastq_names(s)):
            self._move(from_path, s.project.fastq_path / to_name)


    def _move_analysis(self, s: Sample):
        src = self.analysis_dir / 'Data' / s.app_dir() / s.samplesheet_sample_id
        dest = s.project.analysis_path / s.new_sample_id()
        self._move(src, dest)


    def _original_fastq_names(self, s: Sample) -> List[str]:
        comp = 'ora' if s.ora_compression else 'gz'
        names = []
        for r in range(1, s.num_data_reads+1):
            names.append(f"{s.samplesheet_sample_id}_S{s.samplesheet_position}_L{str(s.lane).zfill(3)}_R{r}_001.fastq.{comp}")
        for i in range(1, s.num_index_reads_written_as_fastq+1):
            names.append(f"{s.samplesheet_sample_id}_S{s.samplesheet_position}_L{str(s.lane).zfill(3)}_I{i}_001.fastq.{comp}")
        return names


    def _original_fastq_paths(self, s: Sample) -> List[Path]:
        sp = s.project.samplesheet_sample_project
        if self.is_onboard and sp:
            # Onboard DRAGEN: sample project directories
            base = self.analysis_dir / 'Data' / sp / s.app_dir()  / ('ora_fastq' if s.ora_compression else 'fastq') / sp
        else:
            base = self.analysis_dir / 'Data' / s.app_dir() / ('ora_fastq' if s.ora_compression else 'fastq')
        paths = []
        for fastq_name in self._original_fastq_names(s):
            paths.append(base / fastq_name)
        return paths


    def _dest_fastq_names(self, s: Sample) -> List[str]:
        if s.project.enable_rename_to_sample_name():
            # NSC and MIK renames to plain sample names instead of "samplesheet_sample_id".
            # (this logic ignores the on/offboard status, as the final name should be the same regardless)
            names = []
            comp = 'ora' if s.ora_compression else 'gz'
            for r in range(1, s.num_data_reads+1):
                names.append(f"{s.new_sample_id()}_S{s.samplesheet_position}_L{str(s.lane).zfill(3)}_R{r}_001.fastq.{comp}")
            for i in range(1, s.num_index_reads_written_as_fastq+1):
                names.append(f"{s.new_sample_id()}_S{s.samplesheet_position}_L{str(s.lane).zfill(3)}_I{i}_001.fastq.{comp}")
            return names
        else:
            return self._original_fastq_names(s)


    def _original_analysis(self, s: Sample) -> Path:
        assert self.is_onboard, "Analysis folder is only available with onboard DRAGEN"
        sample_project = s.project.samplesheet_sample_project
        if sample_project: # Sample_Project was not enabled, it is not fully tested
            return self.analysis_dir / 'Data' / sample_project / s.samplesheet_sample_id / s.app_dir() / s.samplesheet_sample_id
        else:
            return self.analysis_dir / 'Data' / s.samplesheet_sample_id / s.app_dir() / s.samplesheet_sample_id


    def link_sav_files(self):
        """Link the SAV files into all required target locations"""

        # Get set of unique destination locations for run QC. Don't copy SAV files to MIK projects,
        target_paths = set(project.run_qc_path for project in self.projects.values() if not project.is_mik())

        for target_path in target_paths:
            logger.info(f"Linking run-level QC files to {target_path}")

            # SAV files
            for fname in ['RunParameters.xml','RunInfo.xml']:
                src = self.run_dir / fname
                dst = target_path / fname
                if src.exists() and not dst.exists():
                    os.link(src, dst)

            i_src = self.run_dir / 'InterOp'
            i_dst = target_path / 'InterOp'
            i_dst.mkdir(exist_ok=True)
            for binf in i_src.glob('*.bin'):
                tgt = i_dst / binf.name
                if not tgt.exists(): os.link(binf, tgt)


    def copy_all_project_qc(self):
        for project in self.projects.values():
            if project.is_mik():
                self.copy_filtered_demux_qc(project)
            else:
                self.copy_demux_qc(project)


    def copy_demux_qc(self, p: Project):
        """Copy global demux QC files to this project's destination"""

        logger.info(f"Copying QC files for project {p.name}")
        # copy LIMS yaml
        shutil.copy(self.lims_path, p.qc_path)

        # Copy Demultiplex_Stats.csv and other global demux stats into "<qc>/Demux" - or
        # in case of offboard, link the file.
        demux_dst = p.qc_path / 'Demux'
        if self.is_onboard:
            demux_src = self.analysis_dir / 'Data' / 'Demux'
            if demux_src.exists() and not demux_dst.exists():
                shutil.copytree(demux_src, demux_dst, copy_function=os.link)
        else:
            # BCL Convert doesn't create a Demux dir, so we soft-link the files from Reports
            (p.qc_path / "Demux").mkdir()
            (p.qc_path / "Demux" / "Demultiplex_Stats.csv").symlink_to("../BCLConvert/fastq/Reports/Demultiplex_Stats.csv")
            (p.qc_path / "Demux" / "Top_Unknown_Barcodes.csv").symlink_to("../BCLConvert/fastq/Reports/Top_Unknown_Barcodes.csv")

        # summary
        sum_src = self.analysis_dir / 'Data' / 'summary'
        sum_dst = p.qc_path / 'summary'
        if sum_src.exists() and not sum_dst.exists():
            shutil.copytree(sum_src, sum_dst, copy_function=os.link)
        # sample sheet
        ss_src = self.analysis_dir / 'Data' / 'SampleSheet.csv'
        ss_dst = p.qc_path / 'SampleSheet.csv'
        if ss_src.exists():
            shutil.copy(ss_src, ss_dst)
        
        # Always copy "App level" QC, which is BCL Convert Reports separated by Dragen app
        # In case of onboard analysis, this doesn't include Demultiplex_Stats - handled below
        # For plain BCL Convert it contains Demultiplex_Stats.csv as well.
        fastq_dir = "fastq_ora" if p.ora_compression else "fastq" # path component
        project_app_dirs = set([s.app_dir() for s in self.samples if s.project == p])
        # Copy the Reports from BCL Convert. For onboard analysis this is split up in directories
        # for each app.
        for app_dir in project_app_dirs:
            demux_qc_destination = p.qc_path / app_dir / fastq_dir
            if not demux_qc_destination.exists(): # If exists, we assume it has the right info
                demux_qc_destination.mkdir(exist_ok=True, parents=True)
                shutil.copytree(
                    self.analysis_dir / "Data" / app_dir / fastq_dir / "Reports",
                    demux_qc_destination / "Reports",
                    copy_function=os.link
                )
            if self.is_onboard:
                shutil.copytree(
                    self.analysis_dir / "Data" / app_dir / "AggregateReports",
                    p.qc_path / app_dir / "AggregateReports",
                    copy_function=os.link
                )


    def _filter_file(self, source_path: Path, dest_path: Path, project: Project):
        sample_ids = set(sample.samplesheet_sample_id for sample in self.samples if sample.project == project)
        with open(source_path) as source:
            with open(dest_path, 'w') as dest:
                header = True
                id_idx = -1
                project_idx = -1
                for line in source:
                    parts = line.strip("\n").split(",")
                    if header:
                        id_idx = parts.index("SampleID")
                        try:
                            project_idx = parts.index("Sample_Project")
                        except ValueError:
                            pass # No project is okay
                        header = False
                        dest.write(line)
                    else:
                        if len(parts) > max(id_idx, project_idx):
                            if project_idx == -1 or parts[project_idx] == project.name:
                                if parts[id_idx] in sample_ids:
                                    dest.write(line)


    def copy_filtered_demux_qc(self, p: Project):
        """Copy specific demultiplexing stats files, but include only the specified project's samples"""

        logger.info(f"Copying and filtering QC files for project {p.name}")

        # Path component details to determine the location of source demultiplex stats files
        fastq_dir = "fastq_ora" if p.ora_compression else "fastq" # path component
        project_app_dirs = set([s.app_dir() for s in self.samples if s.project == p])
        if len(project_app_dirs) != 1:
            raise RuntimeError("Project with filtered demux QC file can only have one app, found: " +
                                ", ".join(str(q) for q in project_app_dirs))
        project_app_dir = next(iter(project_app_dirs))
        
        if self.is_onboard:
            demux_src = self.analysis_dir / 'Data' / 'Demux' / 'Demultiplex_Stats.csv'
        else:
            demux_src = self.analysis_dir / 'Data' / project_app_dir / fastq_dir / 'Reports' / 'Demultiplex_Stats.csv'
        demux_dst = p.qc_path / 'Demultiplex_Stats.csv'
        self._filter_file(demux_src, demux_dst, p)
        qual_src = self.analysis_dir / 'Data' / project_app_dir / fastq_dir / 'Reports' / 'Quality_Metrics.csv'
        qual_dst = p.qc_path / 'Quality_Metrics.csv'
        self._filter_file(qual_src, qual_dst, p)


    def _move(self, src: Path, dest: Path):
        if TEST_MODE:
            logger.info(f"[TEST] mv {src} -> {dest}")
            return
        if not src.exists():
            msg = f"Missing source: {src}"
            if IGNORE_MISSING:
                logger.warning(msg + " (skipped)")
                return
            logger.error(msg)
            sys.exit(1)
        if dest.exists():
            msg = f"Destination exists: {dest}"
            if IGNORE_EXISTING:
                logger.warning(msg + " (skipped)")
                return
            logger.error(msg)
            sys.exit(1)
        src.rename(dest)
        logger.info(f"Moved {src} -> {dest}")


    def run(self):
        self.load_lims_file()
        self.check_sources_and_destinations()
        self.prepare_directories()
        self.link_sav_files()
        self.copy_all_project_qc()
        self.move_sample_files()


def main():
    if len(sys.argv) != 2:
        print(f"Usage: python {Path(sys.argv[0]).name} <ANALYSIS_DIR>")
        sys.exit(1)
    analysis_dir = Path(sys.argv[1])
    FileMover(analysis_dir, DEST_PATHS).run()

if __name__ == '__main__':
    main()
