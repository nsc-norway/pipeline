import unittest
from mock import patch
import sys
import json

sys.path.append('..')
from common import taskmgr, nsc
from genologics.lims import *


class DummyTestCase(unittest.TestCase):
    def test_dummy(self):
        self.assertEquals("moo", "moo")


### Support code

class TaskTestCase(unittest.TestCase):
    pass


class ScriptTestCase(TaskTestCase):
    pass


### Test cases

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
        testargs = ["script", "files/run/180502_NS500336_001_ANOINDEX", "--sample-sheet=files/samplesheet/no-index.csv"]
        with patch.object(sys, 'argv', testargs):
            task.__enter__()
            task.running()
            self.assertEquals(task.projects, correct_projects)


class TestSampleSheetParsing(TaskTestCase):
    pass


class Test10CopyRun(TaskTestCase):
    module = "10_copy_run"

    def test_copy_run_rsync_called(self):
        pass


if __name__ == "__main__":
    unittest.main()
