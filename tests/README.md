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

  * `illuminate`

(illuminate is not a strict runtime dependency of the pipeline, since it can
get stats from text files and from LIMS also. Due to the targeted nature of the
tests, illuminate is absolutely required for running the tests)


### Running the tests ###

The tests can be executed using the following command:

    python test_module.py

Python 2.7 is required to run the tests, and some of the Python package 
dependencies of the pipeline may also be required.

The tests will only run on Unix-like platforms, with slash as path 
separator.

To run only one specific test, invoke test_module.py with the class name and the 
function name:

    python test_module.py Test90PrepareDelivery.test_diag_delivery_nsq


### Debugging mode ###

To troubleshoot test failures, set the environment variable DEBUG=true. E.g.
run this:

    DEBUG=true python test_module.py

This will disable deletion of the test directories, and output the path for
each test.
