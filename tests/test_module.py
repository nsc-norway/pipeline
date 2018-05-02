import unittest
from mock import patch
import sys
import json

sys.path.append('..')
from common import taskmgr, nsc
nsc.SITE = None # Note: this isn't quite effective, how to set it up?
from genologics.lims import *


class DummyTestCase(unittest.TestCase):
    def test_dummy(self):
        self.assertEquals("moo", "moo")


### Support code

class TaskTestCase(unittest.TestCase):
    pass


class ScriptTestCase(TaskTestCase):
    pass


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
        testargs = ["script", "files/run/180502_NS500336_001_ANOINDEX",
                "--sample-sheet=files/samplesheet/no-index.csv"]
        with patch.object(sys, 'argv', testargs):
            task.__enter__()
            task.running()
            self.assertEquals(projects_to_dicts(task.projects), correct_projects)


class TestSampleSheetParsing(TaskTestCase):
    pass


class Test10CopyRun(TaskTestCase):
    module = "10_copy_run"

    def test_copy_run_rsync_called(self):
        pass

# 2. Test of the individual "Task" scipts



if __name__ == "__main__":
    unittest.main()
