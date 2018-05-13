import unittest
from mock import patch
import sys
import json
import tempfile
import os
import subprocess
import string
import random
import shutil

sys.path.append('..')
from common import taskmgr, nsc, samples, remote
nsc.SITE = None # Note: this isn't quite effective, how to set it up?
nsc.REMOTE_MODE = "local"
nsc.BCL2FASTQ_USE_D_OPTION = False
from genologics.lims import *


class DummyTestCase(unittest.TestCase):
    def test_dummy(self):
        self.assertEquals("moo", "moo")


### Support code

class TaskTestCase(unittest.TestCase):
    def setUp(self):
        self.task =  taskmgr.Task(
                    self.module.TASK_NAME,
                    self.module.TASK_DESCRIPTION,
                    self.module.TASK_ARGS
                    )
        self.patcher = patch.object(self.task, 'success_finish')
        self.patcher.start()
        self.task.__enter__()
        self.tempparent = None

    def make_tempdir(self, run_id):
        self.tempparent = tempfile.mkdtemp()
        self.tempdir = os.path.join(self.tempparent, run_id)
        logdir = os.path.join(self.tempdir, "DemultiplexLogs")
        os.mkdir(self.tempdir)
        os.mkdir(logdir)

    def make_qc_dir(self, run_id="180502_E00401_001_BQCTEST"):
        self.tempparent = tempfile.mkdtemp()
        self.tempdir = os.path.join(self.tempparent, run_id)
        shutil.copytree(os.path.join("files/run", run_id), self.tempdir)

    def tearDown(self):
        self.patcher.stop()
        try:
            os.unlink("../{}.pyc".format(self.module.__name__))
        except OSError:
            pass
        if self.tempparent:
            shutil.rmtree(self.tempparent)


def convert_strings_to_unicode(dic):
    res = {}
    for k, v in dic.items():
        if isinstance(v, str):
            res[unicode(k)] = unicode(v)
        else:
            res[unicode(k)] = v
    return res


def projects_to_dicts(projects):
    """Convert a list of project objects to dicts with items
    equal to the attributes, except that the samples and files are
    also converted in this way.
    """
    project_dicts = []
    for project in projects:
        project_dict = convert_strings_to_unicode(project.__dict__)
        project_dict['samples'] = []
        for sample in project.samples:
            sample_dict = convert_strings_to_unicode(sample.__dict__)
            sample_dict['files'] = []
            for file in sample.files:
                sample_dict['files'].append(
                        convert_strings_to_unicode(file.__dict__)
                        )
            project_dict['samples'].append(sample_dict)
        project_dicts.append(project_dict)
    return project_dicts


### Test case ###

# 1. Targeted unit-like tests of some specific, fragile components.
#    (most code is only tested implicitly in the full system tests
#    below)

class TestTaskFramework(unittest.TestCase):

    def test_args_workdir_lanes_no_lims(self):
        """Test some argument parsing functionality."""

        TEST_RUN = "180502_M02980_0155_000000000-BR5PW"
        task = taskmgr.Task("TEST_NAME", "TEST_DESCRIPTION", ["work_dir", "lanes"])
        testargs = ["script", "/tmp/dummy/path/{}".format(TEST_RUN), "--lanes=1"]
        with patch.object(sys, 'argv', testargs):
            task.__enter__()
            task.running()
            self.assertEquals(task.run_id, TEST_RUN)
            self.assertEquals(task.lanes, [1])
            self.assertEquals(task.process, None)


    def test_args_lims_mode(self):
        """Test process arg (this doesn't actually test LIMS interaction)"""

        with patch.object(nsc, 'get_lims'):
            task = taskmgr.Task("TEST_NAME", "TEST_DESCRIPTION", ["work_dir", "lanes"])
            testargs = ["script", "--pid=TEST_ID"]
            with patch.object(sys, 'argv', testargs):
                task.__enter__()
                task.running()
                self.assertIsNotNone(task.process)


    def test_sample_sheet_parsing_no_index(self):
        with open("files/samples/no-index.json") as jsonfile:
            correct_projects = json.load(jsonfile)
        task = taskmgr.Task("TEST_NAME", "TEST_DESCRIPTION", ["work_dir", "sample_sheet"]) 
        RUN_DIR = "files/run/180502_NS500336_001_ANOINDEX"
        testargs = ["script", RUN_DIR, "--sample-sheet=files/samplesheet/no-index.csv"]
        with patch.object(sys, 'argv', testargs):
            task.__enter__()
            task.running()
            self.assertEquals(projects_to_dicts(task.projects), correct_projects)


    def test_sample_sheet_parsing_with_index_merged_lanes(self):
        with open("files/samples/indexed-merged.json") as jsonfile:
            correct_projects = json.load(jsonfile)
        task = taskmgr.Task("TEST_NAME", "TEST_DESCRIPTION", ["work_dir", "sample_sheet"]) 
        RUN_DIR = "files/run/180502_NS500336_001_AINDEXMERGED"
        testargs = ["script", RUN_DIR, "--sample-sheet=files/samplesheet/ns-indexed.csv"]
        with patch.object(sys, 'argv', testargs):
            task.__enter__()
            task.running()
            self.assertEquals(projects_to_dicts(task.projects), correct_projects)


    def test_sample_sheet_parsing_with_extra_comma(self):
        with open("files/samples/indexed-merged.json") as jsonfile:
            correct_projects = json.load(jsonfile)
        task = taskmgr.Task("TEST_NAME", "TEST_DESCRIPTION", ["work_dir", "sample_sheet"]) 
        RUN_DIR = "files/run/180502_NS500336_001_AINDEXMERGED"
        testargs = ["script", RUN_DIR, "--sample-sheet=files/samplesheet/ns-indexed-spreadsheet.csv"]
        with patch.object(sys, 'argv', testargs):
            task.__enter__()
            task.running()
            self.assertEquals(projects_to_dicts(task.projects), correct_projects)


    # TODO add test sample sheet parsnip for QC test run (complex HiSeq run with different
    # indexing strategies)


# 2. Test of the individual "Task" scipts

class Test10CopyRun(TaskTestCase):
    module = __import__("10_copy_run")

    def test_copy_run(self):
        """Test rsync and mkdir called with specific args."""

        RUN_ID = "180502_NS500336_001_ANOINDEX"
        SOURCE_DIR = "files/run/{}".format(RUN_ID)
        parent_dir = tempfile.mkdtemp()
        output_dir = os.path.join(parent_dir, RUN_ID)
        try:
            testargs = ["script", SOURCE_DIR, output_dir]
            with patch.object(sys, 'argv', testargs):
                self.module.main(self.task)

                self.task.success_finish.assert_called_once()
                self.assertTrue(os.path.isfile(os.path.join(output_dir, "Logs", "Test.log")))
                self.assertTrue(os.path.isfile(os.path.join(output_dir, "InterOp", "Test.bin")))
                self.assertTrue(os.path.isfile(os.path.join(output_dir, "RunInfo.xml")))
                self.assertTrue(os.path.isdir(os.path.join(output_dir, "Data", "Intensities", "BaseCalls")))
                self.assertFalse(os.path.exists(os.path.join(output_dir, "Data", "Intensities", "BaseCalls",
                    "L001", "C1.1", "s_1_1101.bcl.gz")))
        finally:
            shutil.rmtree(parent_dir)


class Test20PrepareSampleSheet(TaskTestCase):
    module = __import__("20_prepare_sample_sheet")

    def test_prepare_sample_sheet(self):
        """Testing that the script just writes out an identical sample sheet."""

        RUN_ID = "180502_NS500336_001_ANOINDEX"
        INPUT_SAMPLE_SHEET = "files/samplesheet/ns-indexed.csv"
        self.make_tempdir(RUN_ID)
        shutil.copy(INPUT_SAMPLE_SHEET, os.path.join(self.tempdir, "SampleSheet.csv"))
        testargs = ["script", self.tempdir]
        with patch.object(sys, 'argv', testargs):
            self.module.main(self.task)
            self.task.success_finish.assert_called_once()
        old_samplesheet = open(INPUT_SAMPLE_SHEET).read()
        new_samplesheet = open(os.path.join(self.tempdir, "DemultiplexingSampleSheet.csv")).read()
        self.assertEquals(old_samplesheet, new_samplesheet)


class Test30Demultiplexing(TaskTestCase):
    module = __import__("30_demultiplexing")

    def test_demultiplexing(self):
        """Test that the script calls bcl2fastq2."""

        RUN_ID = "180502_NS500336_001_ANOINDEX"
        SOURCE_DIR = "files/run/{}".format(RUN_ID)

        self.make_tempdir(RUN_ID)
        with open(os.path.join(self.tempdir, "DemultiplexingSampleSheet.csv"), "w"):
            pass

        testargs = ["script", SOURCE_DIR, self.tempdir]
        with patch.object(sys, 'argv', testargs), patch('subprocess.call') as call:
            call.return_value = 0
            self.module.main(self.task)

            self.task.success_finish.assert_called_once()
            expected_args = ['--runfolder-dir', SOURCE_DIR, '--sample-sheet',
                                os.path.join(self.tempdir, 'DemultiplexingSampleSheet.csv'),
                                '--no-lane-splitting', '--output-dir',
                                os.path.join(self.tempdir, 'Data/Intensities/BaseCalls'),
                                '-r', '4', '-p', '16', '-w', '4']
            self.assertTrue(subprocess.call.call_args[0][0][1:] == expected_args)


class Test40MoveResults(TaskTestCase):
    module = __import__("40_move_results")

    def test_move_results(self):
        """Test moving files in a temp directory."""

        RUN_ID = "180502_NS500336_001_AINDEXMERGED"
        INPUT_SAMPLE_SHEET = "files/samplesheet/ns-indexed.csv"
        tempparent = tempfile.mkdtemp()
        local_tempdir = os.path.join(tempparent, RUN_ID)
        try:
            shutil.copytree("files/run/{}".format(RUN_ID), local_tempdir)
            shutil.copy(INPUT_SAMPLE_SHEET, os.path.join(local_tempdir, "DemultiplexingSampleSheet.csv"))
            os.mkdir(os.path.join(local_tempdir, "DemultiplexLogs"))
            testargs = ["script", local_tempdir]

            with patch.object(sys, 'argv', testargs):
                self.module.main(self.task)
                projects = self.task.projects
                samples.flag_empty_files(projects, local_tempdir)
                self.assertTrue(all(not f.empty for p in projects for s in p.samples for f in s.files))
                self.task.success_finish.assert_called_once()
        finally:
            shutil.rmtree(tempparent)

i
class Test50QcAnalysis(TaskTestCase):
    module = __import__("50_qc_analysis")

    def test_qc_analysis(self):
        """Test that QC analysis script starts jobs for all the files"""

        self.make_qc_dir()
        testargs = ["script", self.tempdir]
        with patch.object(sys, 'argv', testargs), patch('subprocess.call') as call:
            call.return_value = 0
            self.module.main(self.task)
            program_args = set()
            for args in call.call_args:
                print args
            self.assertTrue(all(not f.empty for p in projects for s in p.samples for f in s.files))
            self.task.success_finish.assert_called_once()


class Test60PPP(TaskTestCase):
    pass



if __name__ == "__main__":
    unittest.main()

