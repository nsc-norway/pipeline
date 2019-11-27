import unittest
from mock import patch, Mock, call, ANY
import sys
import json
import tempfile
import os
import re
import gzip
import subprocess
import string
import random
import shutil
import glob
from contextlib import contextmanager
sys.path.append('..')

from common import nsc
nsc.SITE = None # Note: this isn't quite effective, how to set it up?
nsc.REMOTE_MODE = "local"
nsc.BCL2FASTQ_USE_D_OPTION = False
DEBUG = os.environ.get('DEBUG') == 'true'

from common import taskmgr, samples, remote
from genologics.lims import *


class DummyTestCase(unittest.TestCase):
    def test_dummy(self):
        self.assertEquals("moo", "moo")


### Support code

class TaskTestCase(unittest.TestCase):

    H4RUN = "180502_E00401_0001_BQCTEST"
    NSRUN = "180502_NS500336_0001_AHTJFWBGX5"
    NOVRUN = "191119_A00943_0005_AHMNCHDMXX"

    def setUp(self):
        self.task =  taskmgr.Task(
                    self.module.TASK_NAME,
                    self.module.TASK_DESCRIPTION,
                    self.module.TASK_ARGS
                    )
        success_patcher = patch.object(self.task, 'success_finish')
        success_patcher.start()
        self.addCleanup(success_patcher.stop)
        # Disable multiprocessing using Pool everywhere
        object_mock = Mock()
        object_mock.map = map
        fake_pool = Mock(return_value=object_mock)
        pool_map_patcher = patch('multiprocessing.Pool', fake_pool)
        pool_map_patcher.start()
        self.addCleanup(pool_map_patcher.stop)
        self.task.__enter__()
        self.tempparent = None
        
    def make_tempdir(self, run_id):
        self.tempparent = tempfile.mkdtemp()
        self.tempdir = os.path.join(self.tempparent, run_id)
        logdir = os.path.join(self.tempdir, "DemultiplexLogs")
        os.mkdir(self.tempdir)
        os.mkdir(logdir)

    @contextmanager
    def qc_dir(self, run_id):
        self.tempparent = tempfile.mkdtemp()
        self.tempdir = os.path.join(self.tempparent, run_id)
        self.basecalls = os.path.join(self.tempdir, "Data", "Intensities", "BaseCalls")
        self.qualitycontrol = os.path.join(self.basecalls, "QualityControl")
        shutil.copytree(os.path.join("files/runs", run_id), self.tempdir)
        # For NovaSeq we have to compress the ConversionStats.xml file, because it is very large
        conversion_stats_file = os.path.join(self.basecalls, "Stats", "ConversionStats.xml")
        compressed_file  = conversion_stats_file + ".gz"
        if os.path.exists(compressed_file):
            with gzip.open(compressed_file) as inf:
                with open(conversion_stats_file, "w") as outf:
                    outf.write(inf.read())
        do_cleanup = False
        try:
            yield self.tempdir
            do_cleanup = not DEBUG
        finally:
            if do_cleanup:
                shutil.rmtree(self.tempparent)
            else:
                print self.__class__.__name__, ">>>", self.tempparent,\
                        "<<< Test dir preserved due to failure or debug mode"

    def check_files_with_reference(self, test_dir, ref_dir):
        self.assertTrue(os.path.isdir(test_dir), "Expected {0} to be a directory, but it isn't.".format(
                    test_dir))
        for name in os.listdir(ref_dir):
            if not name.startswith("."):
                ref_path = os.path.join(ref_dir, name)
                test_path = os.path.join(test_dir, name)
                if os.path.isdir(ref_path):
                    self.check_files_with_reference(test_path, ref_path)
                elif os.path.isfile(ref_path):
                    self.assertTrue(os.path.isfile(test_path), "{0} is not a file".format(test_path))
                    with open(ref_path) as ref_file,\
                            open(test_path) as test_file:
                        test_data = test_file.read()
                        self.assertEquals(ref_file.read(), test_data, "File {0} differs from the "
                                "reference {1}.".format(test_path, ref_path))

    def tearDown(self):
        try:
            os.unlink("../{}.pyc".format(self.module.__name__))
        except OSError:
            pass


@contextmanager
def chdir(path):
    old_dir = os.getcwd()
    os.chdir(path)
    try:
        yield None
    finally:
        os.chdir(old_dir)


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
        task = taskmgr.Task("SsParseNoIndex", "TEST_DESCRIPTION", ["work_dir", "sample_sheet"]) 
        RUN_DIR = "files/runs/180502_NS500336_0001_ANOINDEX"
        testargs = ["script", RUN_DIR, "--sample-sheet=files/samplesheet/no-index.csv"]
        with patch.object(sys, 'argv', testargs):
            task.__enter__()
            task.running()
            self.assertEquals(projects_to_dicts(task.projects), correct_projects)


    def test_sample_sheet_parsing_with_index_merged_lanes(self):
        with open("files/samples/indexed-merged.json") as jsonfile:
            correct_projects = json.load(jsonfile)
        task = taskmgr.Task("SsParseMerged", "TEST_DESCRIPTION", ["work_dir", "sample_sheet"]) 
        RUN_DIR = "files/runs/180502_NS500336_0001_AINDEXMERGED"
        testargs = ["script", RUN_DIR, "--sample-sheet=files/samplesheet/ns-indexed.csv"]
        with patch.object(sys, 'argv', testargs):
            task.__enter__()
            task.running()
            self.assertEquals(projects_to_dicts(task.projects), correct_projects)


    def test_sample_sheet_parsing_with_extra_comma(self):
        with open("files/samples/indexed-merged.json") as jsonfile:
            correct_projects = json.load(jsonfile)
        task = taskmgr.Task("SsParseCommas", "TEST_DESCRIPTION", ["work_dir", "sample_sheet"]) 
        RUN_DIR = "files/runs/180502_NS500336_0001_AINDEXMERGED"
        testargs = ["script", RUN_DIR, "--sample-sheet=files/samplesheet/ns-indexed-spreadsheet.csv"]
        with patch.object(sys, 'argv', testargs):
            task.__enter__()
            task.running()
            self.assertEquals(projects_to_dicts(task.projects), correct_projects)

    def test_sample_sheet_parsing_hi4000(self):
        with open("files/samples/hi4000.json") as jsonfile:
            correct_projects = json.load(jsonfile)
        task = taskmgr.Task("SsParseHi4000", "TEST_DESCRIPTION", ["work_dir", "sample_sheet"]) 
        RUN_DIR = "files/runs/180502_E00401_0001_BQCTEST"
        testargs = ["script", RUN_DIR,
                "--sample-sheet=files/runs/180502_E00401_0001_BQCTEST/DemultiplexingSampleSheet.csv"]
        with patch.object(sys, 'argv', testargs):
            task.__enter__()
            task.running()
            self.assertEquals(projects_to_dicts(task.projects), correct_projects)

    def test_sample_sheet_parsing_novs2std_nonmerged(self):
        with open("files/samples/novs2standard.json") as jsonfile:
            correct_projects = json.load(jsonfile)
        task = taskmgr.Task("SsParseNovS2Std", "TEST_DESCRIPTION", ["work_dir", "sample_sheet"]) 
        RUN_DIR = "files/runs/191119_A00943_0005_AHMNCHDMXX"
        testargs = ["script", RUN_DIR,
                "--sample-sheet=files/runs/191119_A00943_0005_AHMNCHDMXX/DemultiplexingSampleSheet.csv"]
        with patch.object(sys, 'argv', testargs):
            task.__enter__()
            task.running()
            self.assertEquals(projects_to_dicts(task.projects), correct_projects)


# 2. Test of the individual "Task" scipts

class Test10CopyRun(TaskTestCase):
    module = __import__("10_copy_run")

    def test_copy_run(self):
        """Test rsync and mkdir called with specific args."""

        RUN_ID = "180502_NS500336_0001_ANOINDEX"
        SOURCE_DIR = "files/runs/{}".format(RUN_ID)
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

        RUN_ID = "180502_NS500336_0001_ANOINDEX"
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

    def test_novaseq_samplesheet_edited(self):
        """Testing that the script replaced underscores with dashes."""

        RUN_ID = "191119_A00943_0005_AHMNCHDMXX"
        INPUT_SAMPLE_SHEET = "files/samplesheet/NV0000001-LIB.csv"
        self.make_tempdir(RUN_ID)
        shutil.copy(INPUT_SAMPLE_SHEET, os.path.join(self.tempdir, "SampleSheet.csv"))
        testargs = ["script", self.tempdir]
        with patch.object(sys, 'argv', testargs):
            self.module.main(self.task)
            self.task.success_finish.assert_called_once()
        correct_samplesheet = open("files/fasit/20_prepare_sample_sheet/novaseq-standard-fixed.csv").read()
        new_samplesheet = open(os.path.join(self.tempdir, "DemultiplexingSampleSheet.csv")).read()
        self.assertEquals(correct_samplesheet, new_samplesheet)


class Test30Demultiplexing(TaskTestCase):
    module = __import__("30_demultiplexing")

    def test_demultiplexing(self):
        """Test that the script calls bcl2fastq2."""

        RUN_ID = "180502_NS500336_0001_ANOINDEX"
        SOURCE_DIR = "files/runs/{}".format(RUN_ID)

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

        RUN_ID = "180502_NS500336_0001_AINDEXMERGED"
        INPUT_SAMPLE_SHEET = "files/samplesheet/ns-indexed.csv"
        tempparent = tempfile.mkdtemp()
        local_tempdir = os.path.join(tempparent, RUN_ID)
        # Load projects from the json file, will use this to check that the files exist
        # in the correct location after running the script
        with open("files/samples/indexed-merged.json") as jsonfile:
            projects = json.load(jsonfile)
        try:
            # Prepare input directory
            shutil.copytree("files/runs/{}".format(RUN_ID), local_tempdir)
            shutil.copy(INPUT_SAMPLE_SHEET, os.path.join(local_tempdir,
                    "DemultiplexingSampleSheet.csv"))
            os.mkdir(os.path.join(local_tempdir, "DemultiplexLogs"))
            testargs = ["script", local_tempdir]

            with patch.object(sys, 'argv', testargs):
                self.module.main(self.task)
                self.task.success_finish.assert_called_once()
                for project in projects:
                    for sample in project['samples']:
                        for file in sample['files']:
                            # Check that file has been moved correctly
                            self.assertTrue(os.path.exists(os.path.join(local_tempdir,
                                "Data", "Intensities", "BaseCalls", file['path'])))
                            # Also require existence of index read file _I1_ and _I2_, even
                            # though these are not in the json.
                            # This particular test is set up to include index read FASTQ files, as if
                            # bcl2fastq was given the option --create-fastq-for-index-reads.
                            if file['i_read'] == 1:
                                index_1_path = re.sub(r"R1_001.fastq.gz$", "I1_001.fastq.gz", file['path'])
                                self.assertTrue(os.path.exists(os.path.join(local_tempdir,
                                    "Data", "Intensities", "BaseCalls", index_1_path)))
                                index_2_path = re.sub(r"R1_001.fastq.gz$", "I2_001.fastq.gz", file['path'])
                                self.assertTrue(os.path.exists(os.path.join(local_tempdir,
                                    "Data", "Intensities", "BaseCalls", index_2_path)))
        finally:
            shutil.rmtree(tempparent)


class Test50QcAnalysis(TaskTestCase):
    module = __import__("50_qc_analysis")

    def test_qc_analysis(self):
        """Test that QC analysis script starts jobs for all the files"""

        with self.qc_dir(self.H4RUN) as tempdir:
            testargs = ["script", tempdir]
            with patch.object(sys, 'argv', testargs), patch('subprocess.call') as call:
                call.return_value = 0
                self.module.main(self.task)
                # This test is quite incomplete: because the local array job
                # uses subprocess, we don't get the args back! Can only test
                # that it didn't crash.
                self.task.success_finish.assert_called_once()


class Test60DemultiplexStats(TaskTestCase):
    module = __import__("60_demultiplex_stats")

    def test_dx_stats_h4k(self):
        testargs = ["script", "."]
        with patch.object(sys, 'argv', testargs):
            with self.qc_dir(self.H4RUN) as tempdir:
                with chdir(tempdir):
                    self.module.main(self.task)
                self.task.success_finish.assert_called_once()
                self.check_files_with_reference(self.qualitycontrol,
                        "files/fasit/60_demultiplex_stats/h4k")

    def test_dx_stats_nsq(self):
        testargs = ["script", "."]
        with patch.object(sys, 'argv', testargs):
            with self.qc_dir(self.NSRUN) as tempdir:
                with chdir(tempdir):
                    self.module.main(self.task)
                self.task.success_finish.assert_called_once()
                self.check_files_with_reference(self.qualitycontrol,
                        "files/fasit/60_demultiplex_stats/nsq")

    def test_dx_stats_novs2standard(self):
        testargs = ["script", "."]
        with patch.object(sys, 'argv', testargs):
            with self.qc_dir(self.NOVRUN) as tempdir:
                with chdir(tempdir):
                    self.module.main(self.task)
                self.task.success_finish.assert_called_once()
                self.check_files_with_reference(self.qualitycontrol,
                        "files/fasit/60_demultiplex_stats/novs2standard")


class Test60Emails(TaskTestCase):
    module = __import__("60_emails")

    def test_emails_h4k(self):
        with self.qc_dir(self.H4RUN) as tempdir:
            testargs = ["script", tempdir]
            with patch.object(sys, 'argv', testargs):
                self.module.main(self.task)
                self.task.success_finish.assert_called_once()
                self.check_files_with_reference(os.path.join(self.qualitycontrol, "Delivery"),
                        "files/fasit/60_emails/h4k/")

    def test_emails_nsq(self):
        with self.qc_dir(self.NSRUN) as tempdir:
            testargs = ["script", tempdir]
            with patch.object(sys, 'argv', testargs):
                self.module.main(self.task)
                self.task.success_finish.assert_called_once()
                self.check_files_with_reference(os.path.join(self.qualitycontrol, "Delivery"),
                        "files/fasit/60_emails/nsq/")

    def test_emails_novs2standard(self):
        with self.qc_dir(self.NOVRUN) as tempdir:
            testargs = ["script", tempdir]
            with patch.object(sys, 'argv', testargs):
                self.module.main(self.task)
                self.task.success_finish.assert_called_once()
                self.check_files_with_reference(os.path.join(self.qualitycontrol, "Delivery"),
                        "files/fasit/60_emails/novs2standard/")


class Test60Reports(TaskTestCase):
    module = __import__("60_reports")

    def test_reports_h4k(self):
        with open("files/samples/hi4000.json") as jsonfile:
            projects = json.load(jsonfile)
        pdfpaths = []
        for project in projects:
            if not project['is_undetermined']:
                pdir = str(project['proj_dir'])
                for s in project['samples']:
                    for f in s['files']:
                        fname = str(os.path.basename(f['path']))
                        sname = "Sample_" + str(s['name'])
                        qcpath = str(os.path.join(pdir, sname, self.H4RUN + "." + str(f['lane']) + "." + 
                            re.sub(r"fastq\.gz$", "qc.pdf", fname)))
                        pdfpaths.append(qcpath)
        self.reports_general_tester(projects, self.H4RUN, pdfpaths)

    def test_reports_nsq(self):
        with open("files/samples/nsqctest.json") as jsonfile:
            projects = json.load(jsonfile)
        pdfpaths = []
        for project in projects:
            if not project['is_undetermined']:
                pdir = str(project['proj_dir'])
                for s in project['samples']:
                    for f in s['files']:
                        fname = str(os.path.basename(f['path']))
                        qcpath = str(os.path.join(pdir, self.NSRUN + "." + 
                            re.sub(r"fastq\.gz$", "qc.pdf", fname)))
                        pdfpaths.append(qcpath)
        self.reports_general_tester(projects, self.NSRUN, pdfpaths)

    def test_reports_novs2standard(self):
        with open("files/samples/novs2standard.json") as jsonfile:
            projects = json.load(jsonfile)
        pdfpaths = []
        for project in projects:
            if not project['is_undetermined']:
                pdir = str(project['proj_dir'])
                for s in project['samples']:
                    for f in s['files']:
                        fname = str(os.path.basename(f['path']))
                        sname = "Sample_" + str(s['name'])
                        qcpath = str(os.path.join(pdir, sname, self.NOVRUN + "." + str(f['lane']) + "." + 
                            re.sub(r"fastq\.gz$", "qc.pdf", fname)))
                        pdfpaths.append(qcpath)
        self.reports_general_tester(projects, self.NOVRUN, pdfpaths)

    @patch('os.rename')
    def reports_general_tester(self, projects, run_id, pdfpaths, os_rename):
        with self.qc_dir(run_id) as tempdir:
            testargs = ["script", self.tempdir]
            with patch.object(sys, 'argv', testargs),\
                    patch('subprocess.check_call') as sub_call:
                self.module.main(self.task)
                calls = []
                for fp in (str(f['path']) for p in projects for s in p['samples'] for f in s['files']):
                    tex_name = re.sub(r"\.fastq\.gz$", ".qc.tex", os.path.basename(fp))
                    calls.append(call(['/usr/bin/pdflatex', '-shell-escape', tex_name],
                        cwd=os.path.join(self.qualitycontrol, 'pdf'), stdin=ANY, stdout=ANY))
                sub_call.assert_has_calls(calls, any_order=True)
                os_rename.assert_has_calls(
                    [call(ANY, os.path.join(self.basecalls, pdfpath)) for pdfpath in pdfpaths]
                    )
                self.task.success_finish.assert_called_once()


# Test Update LIMS -- Not a priority right now (it's very difficult)


class Test70MultiQC(TaskTestCase):
    """MultiQC script is very simple. Make sure MultiQC tool would be called."""

    module = __import__("70_multiqc")

    def test_multiqc_h4k(self):
        self.multiqc_general_tester("files/samples/hi4000.json", self.H4RUN)

    def test_multiqc_nsq(self):
        self.multiqc_general_tester("files/samples/nsqctest.json", self.NSRUN)

    def test_multiqc_novs2standard(self):
        self.multiqc_general_tester("files/samples/novs2standard.json", self.NOVRUN)

    def multiqc_general_tester(self, json_path, run_id):
        with open(json_path) as jsonfile:
            projects = json.load(jsonfile)
        with self.qc_dir(run_id) as tempdir:
            testargs = ["script", self.tempdir]
            with patch.object(sys, 'argv', testargs), patch('subprocess.call') as sub_call:
                self.module.main(self.task)
                calls = [
                        call(['multiqc', '-q', '-f', 'Stats/'], cwd=self.basecalls)
                        ]
                for project in projects:
                    if project['is_undetermined']:
                        project['name'] = "Undetermined"
                    calls.append(
                        call(['multiqc', '-q', '-f', '.'], cwd=os.path.join(self.qualitycontrol, project['name']))
                        )
                sub_call.assert_has_calls(calls, any_order=True)
                self.task.success_finish.assert_called_once()



class Test80md5sum(TaskTestCase):
    """Check that md5sum was called on all fastq files. It does not currently check that
    the md5sum file also contains the checksums of the QC reports in PDF format."""

    module = __import__("80_md5sum")

    def test_md5_h4k(self):
        with open("files/samples/hi4000.json") as jsonfile:
            projects = json.load(jsonfile)
        with self.qc_dir(self.H4RUN) as tempdir:
            testargs = ["script", tempdir]
            with patch.object(sys, 'argv', testargs), patch('subprocess.call') as sub_call:
                sub_call.return_value = 0
                self.module.main(self.task)
                calls = [] # list of expected calls to md5sum
                for project in projects:
                    if not project['is_undetermined']:
                        files = []
                        # Note: this test is too strict. File names could be in any order. If this
                        # test fails, it may need to be rewritten, but then we can't use assert_has_calls.
                        # Paths for QC PDF files are not tested.
                        for s in project['samples']:
                            for f in s['files']:
                                fname = str(os.path.basename(f['path']))
                                sname = "Sample_" + str(s['name'])
                                fpath = os.path.join(sname, fname)
                                files.append(fpath)
                                files.append(ANY)
                        calls.append(
                            call(['/usr/bin/md5deep', '-rl', '-j5'] + files,
                                cwd=os.path.join(self.basecalls, project['proj_dir']),
                                stderr=ANY, stdout=ANY)
                            )
                sub_call.assert_has_calls(calls, any_order=True)
                self.task.success_finish.assert_called_once()


    def test_md5_nsq(self):
        with open("files/samples/nsqctest-indexfiles.json") as jsonfile:
            projects = json.load(jsonfile)
        with self.qc_dir(self.NSRUN) as tempdir:
            testargs = ["script", tempdir]
            with patch.object(sys, 'argv', testargs), patch('subprocess.call') as sub_call:
                sub_call.return_value = 0
                self.module.main(self.task)
                calls = [] # list of expected calls to md5sum
                for project in projects:
                    if not project['is_undetermined']:
                        files = []
                        # Same note as for HiSeq 4000 applies here. Too specific test.
                        for s in project['samples']:
                            for f in s['files']:
                                fname = str(os.path.basename(f['path']))
                                files.append(fname)
                                files.append(ANY)
                        calls.append(
                            call(['/usr/bin/md5deep', '-rl', '-j5'] + files,
                                cwd=os.path.join(self.basecalls, project['proj_dir']),
                                stderr=ANY, stdout=ANY)
                            )
                sub_call.assert_has_calls(calls, any_order=True)
                self.task.success_finish.assert_called_once()


class Test90PrepareDelivery(TaskTestCase):
    
    module = __import__("90_prepare_delivery")

    def test_hdd_delivery_nsq(self):
        self.hdd_delivery_check(self.NSRUN, "files/samples/nsqctest-indexfiles.json")

    def test_hdd_delivery_h4k(self):
        self.hdd_delivery_check(self.H4RUN, "files/samples/hi4000.json")

    def test_tar_delivery_nsq(self):
        self.tar_delivery_check(self.NSRUN, "files/samples/nsqctest-indexfiles.json",
                "files/fasit/90_prepare_delivery/tar/nsq")

    def test_tar_delivery_h4k(self):
        self.tar_delivery_check(self.H4RUN, "files/samples/hi4000.json",
                "files/fasit/90_prepare_delivery/tar/h4k")

    def test_diag_delivery_nsq(self):
        self.diag_delivery_check(self.NSRUN, "files/samples/nsqctest-indexfiles.json",
                "files/fasit/90_prepare_delivery/diag/nsq")

    def test_diag_delivery_h4k(self):
        self.diag_delivery_check(self.H4RUN, "files/samples/hi4000.json",
                "files/fasit/90_prepare_delivery/diag/h4k")

    def hdd_delivery_check(self, run_id, jsonpath):
        with open(jsonpath) as jsonfile:
            projects = json.load(jsonfile)
        with self.qc_dir(run_id) as tempdir,\
                patch.object(sys, 'argv', ["script", self.tempdir]):
            deliv_test_dir = os.path.join(self.tempparent, "delivery")
            os.mkdir(deliv_test_dir)
            with patch.object(nsc, 'DELIVERY_DIR', deliv_test_dir, create=True),\
                    patch.object(nsc, 'DEFAULT_DELIVERY_MODE', 'User HDD', create=True):
                self.module.main(self.task)
                self.task.success_finish.assert_called_once()
                for project in projects:
                    if not project['is_undetermined']:
                        deliv_project_dir = os.path.join(deliv_test_dir, str(project['proj_dir']))
                        self.check_files_with_reference(deliv_project_dir,
                                "files/runs/{run_id}/Data/Intensities/BaseCalls/{project}".format(
                                    run_id=run_id,
                                    project=project['proj_dir']))

    def tar_delivery_check(self, run_id, jsonpath, ref_dir):
        with open(jsonpath) as jsonfile:
            projects = json.load(jsonfile)
        with self.qc_dir(run_id) as tempdir,\
                patch.object(sys, 'argv', ["script", self.tempdir]):
            deliv_test_dir = os.path.join(self.tempparent, "delivery")
            os.mkdir(deliv_test_dir)
            with patch.object(nsc, 'DELIVERY_DIR', deliv_test_dir, create=True),\
                    patch.object(nsc, 'DEFAULT_DELIVERY_MODE', 'Norstore', create=True),\
                    patch('subprocess.call') as sub_call:
                sub_call.return_value = 0
                self.module.main(self.task)
                self.task.success_finish.assert_called_once()
                expected_calls = []
                for project in projects:
                    if not project['is_undetermined']:
                        proj_dir = str(project['proj_dir'])
                        deliv_project_dir = os.path.join(deliv_test_dir, proj_dir)
                        tar_name = proj_dir + ".tar"
                        expected_calls.append(
                               call(['/bin/tar', 'cf', os.path.join(deliv_project_dir, tar_name),
                                   proj_dir], cwd=self.basecalls, stderr=ANY, stdout=ANY) 
                                )
                        expected_calls.append(
                               call(['/usr/bin/md5deep', '-l', '-j1', tar_name],
                                   cwd=deliv_project_dir, stderr=ANY, stdout=ANY) 
                                )
                self.check_files_with_reference(deliv_test_dir, ref_dir)
                sub_call.assert_has_calls(expected_calls, any_order=True)

    def diag_delivery_check(self, run_id, jsonpath, ref_dir):
        """Diag delivery is quite different from others, and requires copying and renaming of
        QualityControl files. All the code can run locally in test mode, since it's mainly
        rsync and cp. Reference dir is used to confirm compliance."""

        with open(jsonpath) as jsonfile:
            projects = json.load(jsonfile)
        with self.qc_dir(run_id) as tempdir, patch.object(sys, 'argv', ["script", "."]):
            deliv_test_dir = os.path.join(self.tempparent, "delivery")
            os.mkdir(deliv_test_dir)
            with patch.object(nsc, 'DELIVERY_DIR', deliv_test_dir, create=True),\
                    patch.object(nsc, 'DIAGNOSTICS_DELIVERY', deliv_test_dir, create=True),\
                    patch.object(nsc, 'DEFAULT_DELIVERY_MODE', 'Transfer to diagnostics', create=True):
                with chdir(tempdir):
                    self.module.main(self.task)
                self.task.success_finish.assert_called_once()
                self.check_files_with_reference(deliv_test_dir, ref_dir)


if __name__ == "__main__":
    unittest.main()

