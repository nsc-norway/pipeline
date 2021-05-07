# High-level tests for the pipeline #

The script test_module.py contains functional tests for each of the 
data processing / analysis scripts. The tests are implemented in 
Python's built-in `unittest` framework.

Test data files are stored under files/.

### Testing strategy ###

The individual data processing scripts are python scripts in the
root directory, identified by a two-digit and an underscore in the
file name. These scripts make use of the taskmgr module in common/,
specifically the Task class.

The strategy is to test each script as a whole, with a pre-populated
input directory. The output or effects are compared with a reference.
The tests call the scripts in a slightly different way than they are
invoked on the command line: the Task is not used as a context manager.
This is done so that any expections are propagated to the test level,
instead of being caught by the context manager. Other than that, the
tests are designed to be independent of the architecture of the 
scripts. Some additional tests (near the top of the file) are more
integrated with the framework, and work more like unit tests.

LIMS integration is not tested whatsoever. This is too complex to
implement right now, and the goal is anyway to have a shared code
path for LIMS and non-LIMS modes where possible.


### Files ###

  * files/runs/    -- Example run directories, varying degrees of
                      completeness.
  * files/samples/ -- Serialised sample information trees -- parsed 
                      sample sheets.
  * files/samplesheet/ -- Sample sheets for testing the parser.
  * test_module.py -- This file contains all of the test code.
  * tools/         -- Directory containing scripts used to prepare the
                      tests. Not actually part of the testing code.


### Requirements ###

The tests require the `mock` library in order to run.

Some requirements of `pipeline` are mocked out, but the following commands
need to be present:

  * `rsync`
  * `tar`
  * `cp`

If `cp` doesn't support the `-l` option to create hard links, two of the delivery
tests will fail:

  * `test_hdd_delivery_h4k`
  * `test_hdd_delivery_nsq`

Furthermore, this Python library is required in order to test the gathering
of run statistics:

  * `interop`

(interop is not a strict runtime dependency of the pipeline, since it can
get stats from LIMS also. Due to the targeted nature of the
tests, interop is absolutely required for running the tests)


### Running the tests ###

The tests can be executed using the following command:

    python test_module.py

Python 2.7 is required to run the tests, and some of the Python package 
dependencies of the pipeline may also be required: jinja2, requests,
interop. genologics library checked out in same directory as pipeline
(this repo).

The tests will only run on Unix-like platforms, with slash as path 
separator.

To run only one specific test, invoke test_module.py with the class name and the 
function name:

    python test_module.py Test90PrepareDelivery.test_diag_delivery_nsq


### Identify failing test code ###

The output of failed tests look something like this:

    ======================================================================
    FAIL: test_sample_sheet_parsing_no_index (__main__.TestTaskFramework)
    ----------------------------------------------------------------------
    Traceback (most recent call last):
      File "test_module.py", line 192, in test_sample_sheet_parsing_no_index
        self.assertEquals(projects_to_dicts(task.projects), correct_projects)
    AssertionError: Lists differ: [{u'is_undetermined': True, u'... != [{u'is_undetermined': True, u'...
    
    First differing element 0:
    {u'is_undetermined': True, u'proj_dir': None, u'name': None, u'samples': [{u'files': [], u'description': None, u'sample_dir': None, u'sample_index': 0, u'sample_id': None, u'name': None}]}
    {u'is_undetermined': True, u'proj_dir': None, u'name': None, u'samples': [{u'sample_dir': None, u'files': [], u'sample_index': 0, u'name': None, u'sample_id': None}]}
    
    Diff is 1582 characters long. Set self.maxDiff to None to see it.


The line with FAIL: shows the class (`TestTaskFramework`) and the function name
(`test_sample_sheet_parsing_no_index`) of the failing test. The text below that line
may give a clue to the reason for the failure. If not, you will have to check the
code of the failing test in `test_module.py`.

If the test requires a temporary run directory, that directory path will be printed
earlier in the output, in an output like this:

    Test60DemultiplexStats >>> /tmp/tmpDGc1Vs <<< Test dir preserved due to failure or debug mode


### Debugging mode ###

To troubleshoot test failures, set the environment variable DEBUG=true. E.g.
run this:

    DEBUG=true python test_module.py

This will disable deletion of the test directories, and output the path for
each test. However, test directories are automatically preserved on failure,
so this option may not be necessary.


### Updating references ("fasit") ###

In case of expected changes to the pipeline output (or sometimes internal state),
the reference files should be updated to reflect the new situation.

#### Updating the sample sheet-related reference files ####

The sample sheet parser is tested by comparing the parsed sample sheet information
to a set of previous results. The previous results are saved in a json format. These
json files can be generated using the script `tools/dump_projects_json.py`.

1.  Determine the list of files and run ID from the test code.

    Reference file: with open("files/samples/no-index.json") as jsonfile:
    Sample Sheet: testargs = ["script", RUN_DIR, "--sample-sheet=files/samplesheet/no-index.csv"]
    Run ID: RUN_DIR = "files/runs/180502_NS500336_0001_ANOINDEX"

    From this, the correct values are:

    Reference file: "files/samples/no-index.json"
    Sample Sheet: "files/samplesheet/no-index.csv"
    Run ID: "180502_NS500336_0001_ANOINDEX"


2.  Run the sample sheet parser and json dump tool `tools/dump_projects_json.py`. As
    shown in the examples in the top of the file, take care to specify the correct
    parameters as environment variables (`READS`, `COLLAPSE_LANES`). The parameters
    may not be obvious, but can be determined from the run directory in the testing
    code (or by experimentation). The option `COLLAPSE_LANES` must be set to `true`
    to enable.

    Speciy the sample sheet and run ID on the command line, and pipe the output into 
    the json file.
    
    python tools/dump_projects_json.py files/samplesheet/no-index.csv 180502_NS500336_0001_ANOINDEX  > files/samples/no-index.json

3.  Confirm test passing after update. Note that you must specify the right class and
    function name, or run all tests, this is an example:

    python test_module.py TestTaskFramework.test_sample_sheet_parsing_no_index

4.  Commit the updated references.

    
#### Updating other reference files ####

Most files can be updated more directly, by copying the newly produced file into the
location under files/fasit. The path of a failed reference file is usually evident from the
output.

Set the environment variable DEBUG=true to produce a new set of outputs even if the
test does not fail (e.g. in case new files are added, but existing files remain).


