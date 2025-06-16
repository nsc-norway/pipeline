import unittest
import tempfile
import os
import shutil
import importlib
import sys
import importlib.util
import subprocess
from pathlib import Path
from unittest.mock import patch
import yaml

EXAMPLE_YAML = """bcl_convert_version: 4.3.16
compute_platform: Onboard DRAGEN
demultiplexing_process_id: 24-1081653
run_id: 20250502_LH00534_0135_B22LCYYLT4
samples:
- artifact_id: 2-4950921
  artifact_name: 188-450
  delivery_method: TSD project
  lane: 1
  lane_artifact: 2-4976860
  num_data_read_passes: 2
  onboard_workflow: bcl_convert
  ora_compression: false
  output_artifact_id: 92-4976795
  project_id: DAH12001
  project_name: Christophersen-DNA2-2025-04-10
  project_type: Sensitive
  sample_id: DAH12001A188
  sample_name: 188-450
  samplesheet_position: 140
  samplesheet_sample_id: 188-450_2-4950921
- artifact_id: 2-4950789
  artifact_name: 156-416
  delivery_method: TSD project
  lane: 1
  lane_artifact: 2-4976860
  num_data_read_passes: 2
  onboard_workflow: bcl_convert
  ora_compression: false
  output_artifact_id: 92-4976796
  project_id: DAH12001
  project_name: Christophersen-DNA2-2025-04-10
  project_type: Sensitive
  sample_id: DAH12001A156
  sample_name: 156-416
  samplesheet_position: 108
  samplesheet_sample_id: 156-416_2-4950789
- artifact_id: 2-4950912
  artifact_name: 179-441
  delivery_method: TSD project
  lane: 1
  lane_artifact: 2-4976860
  num_data_read_passes: 2
  onboard_workflow: bcl_convert
  ora_compression: false
  output_artifact_id: 92-4976797
  project_id: DAH12001
  project_name: Hei-DNA2-2025-04-10
  project_type: Sensitive
  sample_id: DAH12001A179
  sample_name: 179-441
  samplesheet_position: 131
  samplesheet_sample_id: 179-441_2-4950912
status: ImportCompleted
"""

def create_example_run(tmpdir: Path, yaml_text: str):
    run_id = "20250502_LH00534_0135_B22LCYYLT4"
    run_dir = tmpdir / run_id
    analysis_dir = run_dir / "Analysis" / "1"
    fastq_dir = analysis_dir / "Data" / "BCLConvert" / "fastq"
    reports_dir = fastq_dir / "Reports"
    demux_dir = analysis_dir / "Data" / "Demux"
    aggregate_dir = analysis_dir / "Data" / "BCLConvert" / "AggregateReports"

    for p in [reports_dir, demux_dir, aggregate_dir]:
        p.mkdir(parents=True, exist_ok=True)

    samples = yaml.safe_load(yaml_text)["samples"]
    for entry in samples:
        sid = entry["samplesheet_sample_id"]
        pos = entry["samplesheet_position"]
        lane = entry["lane"]
        # sample analysis dir
        (analysis_dir / "Data" / "BCLConvert" / sid).mkdir(parents=True, exist_ok=True)
        for r in range(1, entry["num_data_read_passes"] + 1):
            (fastq_dir / f"{sid}_S{pos}_L{lane:03d}_R{r}_001.fastq.gz").touch()
    # minimal QC files
    header = "SampleID,Sample_Project\n"
    for f in ["Demultiplex_Stats.csv", "Quality_Metrics.csv", "Top_Unknown_Barcodes.csv"]:
        (reports_dir / f).write_text(header)
    for f in ["Demultiplex_Stats.csv", "Quality_Metrics.csv"]:
        (demux_dir / f).write_text(header)
    (aggregate_dir / "dummy.txt").write_text("dummy")

    # run root files
    interop = run_dir / "InterOp"
    interop.mkdir()
    (interop / "dummy.bin").write_text("bin")
    (run_dir / "RunInfo.xml").write_text("<RunInfo/>")
    (run_dir / "RunParameters.xml").write_text("<RunParameters/>")

    (analysis_dir / "ClarityLIMSImport_NSC.yaml").write_text(yaml_text)
    return run_dir, analysis_dir

class FileMoverTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.old_cwd = Path.cwd()
        os.chdir(self.tmpdir)
        self.run_dir, self.analysis_dir = create_example_run(self.tmpdir, EXAMPLE_YAML)
        os.environ["TEST_MODE"] = "1"
        spec = importlib.util.spec_from_file_location(
            "novaseq_x_file_mover", (Path(__file__).resolve().parent.parent / "novaseq-x-file-mover.py")
        )
        self.fm_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.fm_mod)

    def tearDown(self):
        os.chdir(self.old_cwd)
        shutil.rmtree(self.tmpdir)
        os.environ.pop("TEST_MODE", None)
        sys.modules.pop("novaseq-x-file-mover", None)

    def test_run_creates_directories(self):
        mover = self.fm_mod.FileMover(self.analysis_dir, self.fm_mod.DEST_PATHS)
        with patch("os.link"):
            mover.run()
        for p in mover.projects.values():
            self.assertTrue(p.fastq_path.is_dir())
            self.assertTrue(p.qc_path.is_dir())

class AutomationCronTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.old_cwd = Path.cwd()
        os.chdir(self.tmpdir)
        self.run_dir, self.analysis_dir = create_example_run(self.tmpdir, EXAMPLE_YAML)
        os.environ["TEST_MODE"] = "1"
        spec = importlib.util.spec_from_file_location(
            "novaseq_x_automation", (Path(__file__).resolve().parent.parent / "novaseq-x-automation-cron.py")
        )
        self.ac_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.ac_mod)

    def tearDown(self):
        os.chdir(self.old_cwd)
        shutil.rmtree(self.tmpdir)
        os.environ.pop("TEST_MODE", None)
        sys.modules.pop("novaseq-x-automation-cron", None)

    def test_main_triggers_file_mover(self):
        calls = []
        def fake_run(logger, args, **kw):
            calls.append(args)
            return None
        with patch.object(self.ac_mod, "run_subprocess_with_logging", side_effect=fake_run), \
             patch.object(self.ac_mod, "start_nsc_nextflow", return_value="1"), \
             patch.object(self.ac_mod.subprocess, "run") as sprun:
            sprun.return_value = subprocess.CompletedProcess(["sbatch"], 0, stdout=b"1")
            self.ac_mod.main()
        self.assertTrue(any("novaseq-x-file-mover.py" in c[1] for c in calls))

if __name__ == "__main__":
    unittest.main()
