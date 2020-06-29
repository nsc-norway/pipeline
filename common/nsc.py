# NSC-specific configuration
from __future__ import print_function
from genologics.lims import *
import getpass
import os
import sys

# Configure prod or dev
TAG="prod"

try:
    with open("/etc/pipeline-site") as f: 
        SITE = f.read().strip()
except IOError:
    print("Warning: unknown site, you may set the site in /etc/pipeline-site", file=sys.stderr)
    SITE = None

### Configuration for all sites, for production and dev ###

#### Names of objects in LIMS ####

# UDFs used in the LIMS for tracking automatic processing
AUTO_FLAG_UDF = "NSC Automatic processing"
AUTO_FLOWCELL_UDF = "Automation lane groups"

CURRENT_JOB_UDF = "Current job"
JOB_STATUS_UDF = "Job status"
JOB_STATE_CODE_UDF = "Job state code" #RUNNING,FAILED,COMPLETED
ERROR_DETAILS_UDF = "Error details"

# UDFs for configuration and job steering (On process types)
RUN_ID_UDF = "Run ID"
THREADS_UDF = "Number of threads"
SOURCE_RUN_DIR_UDF = "Source run folder"
WORK_RUN_DIR_UDF = "Working run folder"
OTHER_OPTIONS_UDF = "Other options for bcl2fastq"
NO_LANE_SPLITTING_UDF = "No lane splitting"
LANES_UDF = "Lanes"

# Other UDFs 
# On process types
BCL2FASTQ_VERSION_UDF = "bcl2fastq version"
# On Analyte
LANE_UNDETERMINED_UDF = "NSC % Undetermined Indices (PF)"
# On Project
DELIVERY_METHOD_UDF = "Delivery method"
PROJECT_TYPE_UDF = "Project type"
PROJECT_16S_UDF = "Internal barcode demultiplexing (16S)"
# On Container << to be removed when moving overview page
RECENTLY_COMPLETED_UDF = "Recently completed"
PROCESSED_DATE_UDF = "Processing completed date"


# -Names of inputs/outputs on processes -
# Input files
INPUT_SAMPLE_SHEET = "Input sample sheet"
# Files 
SAMPLE_SHEET = "Demultiplexing sample sheet"
# Output files
BCL2FASTQ_LOG = "bcl2fastq log"
FASTQC_LOG = "fastqc log"
FASTQ_OUTPUT = "{sample_name}"

# Sequencing processes
SEQ_PROCESSES=[
        ('hiseqx', 'Illumina Sequencing (HiSeq X) 1.0'),
        ('hiseq4k', 'Illumina Sequencing (HiSeq 3000/4000) 1.0'),
        ('novaseq', 'AUTOMATED - NovaSeq Run (NovaSeq 6000 v3.0)'),
        ('nextseq', 'NextSeq Run (NextSeq) 1.0'),
        ('miseq', 'MiSeq Run (MiSeq) 5.0'),
        ('miseq', 'MiSeq Run (MiSeq v1.0)')
        ]

DEMULTIPLEXING_QC_PROCESS = "Demultiplexing and QC NSC 2.0"


#### Misc config ####

# Log dir in each run folder
RUN_LOG_DIR="DemultiplexLogs"


#### System config ####

# System programs
RSYNC="/usr/bin/rsync"
MD5DEEP="/usr/bin/md5deep"
PDFLATEX="/usr/bin/pdflatex"


# Some programs aren't put here, because they are standard on all 
# machines: tar, cp.


# Tag to identify LIMS server -- Default depends on TAG and SITE, used in get_lims() at the 
# bottom of this file.
LIMS_SERVER=None

### Common / default program paths ###
# MultiQC: Use default path
MULTIQC = ["multiqc"]

### Default configuration parameters ###
# The -d option is required for bcl2fastq versions < 2.19

OPEN_EMAILS_SCRIPT = "/data/runScratch.boston/scripts/Open_emails.command"

### Site specific configuration ###

if SITE and SITE.startswith("cees"):
    # Data processing/analysis programs
    BCL2FASTQ2="/usr/local/bin/bcl2fastq"
    FASTQC="/opt/FastQC/fastqc"
    FASTDUP="/opt/nsc/bin/fastdup"
    SUPRDUPR=False
    if SITE == "cees-sensitive":
        MULTIQC = ["/opt/rh/python27/root/usr/bin/multiqc"]
    BASEURI="https://cees-lims.sequencing.uio.no"

    REMOTE_MODE = "local"
    DEFAULT_DELIVERY_MODE="Norstore"

elif SITE == "ous":
    # Data processing/analysis programs
    #BCL2FASTQ2="/data/common/tools/nscbin/bcl2fastq"
    BCL2FASTQ2="/data/common/tools/bcl2fastq/bcl2fastq2-v2.20.0/nscinstallbin/bin/bcl2fastq"
    FASTQC="/data/common/tools/fastQC/FastQC_v0.11.8/fastqc"
    FASTDUP=False
    SUPRDUPR=["/data/common/tools/suprDUPr/v1.3/suprDUPr", "-s", "10", "-e", "60"]
    MULTIQC = ["/data/common/tools/multiqc/multiqc_1.9--pyh9f0ad1d_0.sif", "multiqc"]
    BASEURI="https://ous-lims.sequencing.uio.no"

    REMOTE_MODE = "srun"
    DEFAULT_DELIVERY_MODE="User HDD"

    # * Command line to run slurm *
    SBATCH_ARGLIST=["/usr/bin/sbatch", "--partition=main", "--qos=high"]
    SQUEUE=["/usr/bin/squeue"]

else:
    BCL2FASTQ2="bcl2fastq"
    FASTQC="fastqc"
    REMOTE_MODE="local"
    FASTDUP="suprDUPr"
    SUPRDUPR=False

FASTDUP_ARGLIST=[FASTDUP, "-s", "10", "-e", "60"]


### Site and phase (TAG) dependent configuration ###

# Paths
if SITE and SITE.startswith("cees"):
    if TAG == "prod":
        PRIMARY_STORAGE = "/storage/nscdata/runsIllumina"
        SECONDARY_STORAGE="/storage/nscdata/runsIllumina"
        if SITE == "cees":
            DELIVERY_DIR="/storage/nscdata/runsIllumina/delivery" 
        elif SITE == "cees-sensitive":
            DELIVERY_DIR="/storage/nscdata_s/runsIllumina/delivery" 
        TRIGGER_DIR="/opt/nsc/trigger"
        LIMS_SERVER="cees-lims"

    elif TAG == "dev":
        PRIMARY_STORAGE = "/var/pipeline-test/runsIllumina"
        SECONDARY_STORAGE="/var/pipeline-test/runsIllumina"
        #TRIGGER_DIR="/opt/nsc/trigger"

elif SITE == "ous":
    PRIMARY_STORAGE = "/data/runScratch.boston"     # source data
    if TAG == "prod":
        SECONDARY_STORAGE="/data/runScratch.boston/demultiplexed"         # location of demultiplexed files
        DELIVERY_DIR="/data/runScratch.boston/demultiplexed/delivery"     # used by prepare-delivery after QC
        DIAGNOSTICS_DELIVERY = "/data/diag/nscDelivery"
        TRIGGER_DIR="/data/runScratch.boston/scripts/trigger"
        LIMS_SERVER="ous-lims"

    elif TAG == "dev":
        SECONDARY_STORAGE="/data/runScratch.boston/test"    # location of demultiplexed files
        DELIVERY_DIR="/data/runScratch.boston/test/delivery"# used by prepare-delivery after QC
        DIAGNOSTICS_DELIVERY = "/data/runScratch.boston/test/diag"
        TRIGGER_DIR="/data/runScratch.boston/scripts/dev/trigger"
        LIMS_SERVER="dev-lims"
        
else:
    PRIMARY_STORAGE = "/tmp"
    SECONDARY_STORAGE = "/tmp"


# Configure LIMS access:
def get_lims(server_id=None):

    server_id = server_id or LIMS_SERVER

    if not server_id:
        # For DEV use the LIMS configured in the ~/.genologicsrc file
        from genologics.config import BASEURI, USERNAME, PASSWORD
        return Lims(BASEURI,USERNAME,PASSWORD)

    else:
        if server_id == "ous-lims":
            url = "https://ous-lims.sequencing.uio.no"
            pw_file = "/data/runScratch.boston/scripts/etc/seq-user/apiuser-password.txt"

        elif server_id == "dev-lims":
            url = "https://dev-lims.sequencing.uio.no"
            pw_file = "/data/runScratch.boston/scripts/etc/seq-user/dev-apiuser-password.txt"

        elif server_id == "cees-lims":
            url = "https://cees-lims.sequencing.uio.no"
            # This could be cleaned up. Only need one file, I don't know which one.
            pw_file = "/opt/gls/clarity/users/glsai/apiuser-password.txt"
            if not os.path.exists(pw_file):
                pw_file = "/opt/nsc/conf/apiuser-password.txt"

        elif server_id == "x-lims":
            url = "https://x-lims.sequencing.uio.no"
            pw_file = "/data/runScratch.boston/scripts/etc/seq-user/x-apiuser-password.txt"

        else:
            pw_file = None
        
        if not pw_file:
            raise RuntimeException("LIMS server ID '{0}' does not exist.".format(server_id))

        return Lims(
                url,
                "apiuser",
                open(pw_file).read().strip()
                )


