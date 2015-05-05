#!/bin/env python

# Cancel slurm job associated with process


import sys, os.path
import subprocess
from genologics.lims import *

from common import nsc, utilities



def main(process_id):
    process = Process(nsc.lims, id=process_id)
    rc = subprocess.call(nsc.SCANCEL_ARGLIST + [str(process.udf[nsc.JOB_ID_UDF])])
    if rc == 0:
        process.udf[nsc.JOB_STATUS_UDF] = "Cancelled"
        process.udf[nsc.JOB_STATE_CODE_UDF] = "CANCELLED"
        process.put()
        sys.exit(0)
    else:
        #print "Cancel command failed"
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1])

