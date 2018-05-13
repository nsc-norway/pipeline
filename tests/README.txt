#### High-level tests for the pipeline ####

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
input directory.

