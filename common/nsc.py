# NSC-specific configuration

from genologics.lims import *
import getpass
import os
from ConfigParser import SafeConfigParser

# Configure prod or dev
TAG="prod"

try:
    with open("/etc/pipeline-site") as f: 
        SITE = f.read().strip()
except IOError:
    print "Warning: unknown site, you may set the site in /etc/pipeline-site"
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
        ('hiseq', 'Illumina Sequencing (Illumina SBS) 5.0'),
        ('nextseq', 'NextSeq Run (NextSeq) 1.0'),
        ('miseq', 'MiSeq Run (MiSeq) 5.0')
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


# Some programs don't have to be put here, because they are standard on all 
# machines: tar.



### Site specific configuration ###

if SITE == "cees":
    # Data processing/analysis programs
    BCL2FASTQ2="/usr/local/bin/bcl2fastq"
    FASTQC="/opt/FastQC/fastqc"
    FASTDUP="/opt/nsc/bin/fastdup"
    BASEURI="https://cees-lims.sequencing.uio.no"

    REMOTE_MODE = "local"
    DEFAULT_DELIVERY_MODE="Norstore"

elif SITE == "ous":
    # Data processing/analysis programs
    #BCL2FASTQ2="/data/common/tools/nscbin/bcl2fastq"
    BCL2FASTQ2="/data/common/tools/bcl2fastq/bcl2fastq2-v2.18.0.12/nscinstallbin/bin/bcl2fastq"
    FASTQC="/data/common/tools/nscbin/fastqc"
    FASTDUP="/data/common/tools/nscbin/fastdup"
    BASEURI="https://ous-lims.sequencing.uio.no"

    REMOTE_MODE = "srun"
    DEFAULT_DELIVERY_MODE="User HDD"

    # * Command line to run slurm *

    # sbatch commands for "scheduler mode". Sudo mode is not supported.
    SBATCH_ARGLIST=["/usr/bin/sbatch"]
    
    # Args for jobs which mainly do I/O on the secondary storage, not processing
    # Set a higher than default priority to make sure they run in preference of 
    # jobs that can run anywhere. (Now less relevant, storage access is the same
    # from all nodes)
    SRUN_STORAGE_JOB_ARGS=["--nice=10"]

    SQUEUE=["/usr/bin/squeue"]

else:
    BCL2FASTQ2="bcl2fastq"
    FASTQC="fastqc"
    REMOTE_MODE="local"
    FASTDUP="fastdup"

FASTDUP_ARGLIST=[FASTDUP, "-s", "10", "-e", "60"]


### Site and phase (TAG) dependent configuration ###

# Paths
if SITE == "cees":
    if TAG == "prod":
        PRIMARY_STORAGE = "/storage/nscdata/runsIllumina"
        SECONDARY_STORAGE="/storage/nscdata/runsIllumina"
        DELIVERY_DIR="/storage/nscdata/runsIllumina/delivery" 
        TRIGGER_DIR="/opt/nsc/trigger"

    elif TAG == "dev":
        PRIMARY_STORAGE = "/var/pipeline-test/runsIllumina"
        SECONDARY_STORAGE="/var/pipeline-test/runsIllumina"
        LOG_DIR = "/data/nsc.loki/automation/dev/logs"
        #TRIGGER_DIR="/opt/nsc/trigger"

elif SITE == "ous":
    PRIMARY_STORAGE = "/data/runScratch.boston"     # source data
    if TAG == "prod":
        SECONDARY_STORAGE="/data/runScratch.boston/demultiplexed"         # location of demultiplexed files
        DELIVERY_DIR="/data/runScratch.boston/demultiplexed/delivery"     # used by prepare-delivery after QC
        DIAGNOSTICS_DELIVERY = "/data/diag/nscDelivery"
        LOG_DIR = "/data/nsc.loki/automation/logs" # logs for copy job (10_... script used at OUS)
        TRIGGER_DIR="/data/nsc.loki/automation/trigger"

    elif TAG == "dev":
        SECONDARY_STORAGE="/data/nsc.loki/test"    # location of demultiplexed files
        DELIVERY_DIR="/data/nsc.loki/test/delivery"# used by prepare-delivery after QC
        DIAGNOSTICS_DELIVERY = "/data/nsc.loki/test/diag"
        LOG_DIR = "/data/nsc.loki/automation/dev/logs"
        TRIGGER_DIR="/data/nsc.loki/automation/dev/trigger"


# Configure LIMS access (should be cleaned up)
if TAG == "dev":
    from genologics.config import *
    lims = Lims(BASEURI,USERNAME,PASSWORD)
elif TAG == "prod":
    if getpass.getuser() == "seq-user":
        pw_file = "/data/nsc.loki/automation/etc/seq-user/apiuser-password.txt"

    elif getpass.getuser() == "glsai":
        pw_file = "/opt/gls/clarity/users/glsai/apiuser-password.txt"
        if not os.path.exists(pw_file):
            pw_file = "/opt/nsc/conf/apiuser-password.txt"

    else:
        pw_file = None
    
    if pw_file: # make sure this can load w/o pw file, for non-lims tasks
        lims = Lims(
                BASEURI,
                "apiuser",
                open(pw_file).read().strip()
                )


