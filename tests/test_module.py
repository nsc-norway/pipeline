import unittest
from mock import patch
import sys
import json
import tempfile
import os
import string
import random
import shutil

sys.path.append('..')
from common import taskmgr, nsc, samples, remote
nsc.SITE = None # Note: this isn't quite effective, how to set it up?
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

    def random_dir(self):
        return os.path.join("/tmp", 
                "".join(random.choice(string.ascii_uppercase + string.digits) for i in range(10))
                )

    def make_tempdir(self, run_id):
        self.tempparent = tempfile.mkdtemp()
        self.tempdir = os.path.join(self.tempparent, run_id)
        logdir = os.path.join(self.tempdir, "DemultiplexLogs")
        os.mkdir(self.tempdir)
        os.mkdir(logdir)

    def tearDown(self):
        self.patcher.stop()
        try:
            os.unlink("../{}.pyc".format(self.module.__name__))
        except OSError:
            pass
        if self.tempparent:
            shutil.rmtree(self.tempparent)



def deep_equals(a, b):
    try:
        a_keys = set(a.__dict__)
        b_keys = set(b.__dict__)
    except AttributeError:
        return a == b
    if a_keys == b_keys:
        return all(deep_equals(a.__dict__[key], b.__dict__[key]) for key in a_keys)

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



class TestRemote(unittest.TestCase):
    pass # TODO: Test remote slurm / local calls



# 2. Test of the individual "Task" scipts

class Test10CopyRun(TaskTestCase):

    module = __import__("10_copy_run")

    def test_copy_run(self):
        """Test rsync and mkdir called with specific args."""

        RUN_ID = "180502_NS500336_001_ANOINDEX"
        SOURCE_DIR = "files/run/{}".format(RUN_ID)
        output_dir = os.path.join(self.random_dir(), RUN_ID)
        testargs = ["script", SOURCE_DIR, output_dir]
        with patch.object(sys, 'argv', testargs):
            with patch('common.remote.run_command') as run_command, patch('os.mkdir') as mkdir:
                run_command.return_value = 0
                self.module.main(self.task)

                self.task.success_finish.assert_called_once()
                mkdir.assert_any_call(output_dir)
                mkdir.assert_any_call(os.path.join(output_dir, 'DemultiplexLogs'))
                remote.run_command.assert_called_once_with(['/usr/bin/rsync', '-rLt', '--chmod=ug+rwX',
                    '--exclude=/Thumbnail_Images', '--exclude=/Images', '--exclude=/Data/Intensities/L00*',
                    '--exclude=/Data/Intensities/BaseCalls/L00*', SOURCE_DIR + "/", output_dir], self.task, 'rsync',
                    '02:00:00', comment=RUN_ID, logfile='{}/DemultiplexLogs/10._copy_run.rsync.txt'.format(output_dir),
                    storage_job=True)


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
        with patch.object(sys, 'argv', testargs), patch('common.remote.run_command') as run_command:
            run_command.return_value = 0
            self.module.main(self.task)

            self.task.success_finish.assert_called_once()
            remote.run_command.assert_called_once_with(['bcl2fastq', '--runfolder-dir', SOURCE_DIR, '--sample-sheet',
                    os.path.join(self.tempdir, 'DemultiplexingSampleSheet.csv'), '--no-lane-splitting', '--output-dir',
                    os.path.join(self.tempdir, 'Data/Intensities/BaseCalls'), '-r', '4', '-p', '16', '-w', '4'],
                    self.task, 'bcl2fastq2', bandwidth='448M', comment=RUN_ID, cpus=16,
                    logfile=os.path.join(self.tempdir, 'DemultiplexLogs/30._demultiplexing.bcl2fastq2.txt'), mem='15G',
                    time='1-0')


class Test40MoveResults(TaskTestCase):

    module = __import__("40_move_results")

    def test_move_results(self):
        """Test moving files in a temp directory."""

        RUN_ID = "180502_NS500336_001_ANOINDEX"
        SOURCE_DIR = "files/run/{}".format(RUN_ID)

        self.make_tempdir(RUN_ID)

        testargs = ["script", self.tempdir]
        with patch.object(sys, 'argv', testargs):
            self.module.main(self.task)
            # TODO incomplete test fails
            self.task.success_finish.assert_called_once()



if __name__ == "__main__":
    unittest.main()

