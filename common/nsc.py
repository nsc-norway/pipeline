# NSC-specific configuration

from genologics.lims import *
import getpass
import os
from ConfigParser import SafeConfigParser

# Configure prod or dev
TAG="dev"

try:
    with open("/etc/pipeline-site") as f: 
        SITE = f.read().strip()
except IOError:
    pass

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
    BASEURI="https://cees-lims.sequencing.uio.no"

    REMOTE_MODE = "local"

elif SITE == "ous":
    # Data processing/analysis programs
    #BCL2FASTQ2="/data/common/tools/nscbin/bcl2fastq"
    BCL2FASTQ2="/data/common/tools/bcl2fastq/bcl2fastq2-v2.17.1.14/nscinstallbin/bin/bcl2fastq"
    FASTQC="/data/common/tools/nscbin/fastqc"
    FASTDUP_ARGLIST=["/data/common/tools/nscbin/fastdup", "-s", "10", "-e", "60"]
    BASEURI="https://ous-lims.sequencing.uio.no"

    REMOTE_MODE = "srun"

    # * Command line to run slurm *

    # Special case to allow glsai to run commands as seq-user through sudo. This is only
    # needed on the development system. In the production environment, the commands are executed
    # on the server loki directly as seq-user.
    # The first example below essentially allows glsai to run any command as seq-user,
    # thus gaining access to the NSC storage volumes. I haven't found a good way to allow
    # the script to set
    # the resources etc., but restrict the command.
    #glsai   ALL=(seq-user)  NOPASSWD:/usr/bin/sbatch
    #Defaults:glsai          !requiretty
    #Defaults:glsai          umask=007,umask_override
    #Defaults:glsai          !logfile
    SRUN_GLSAI_ARGLIST=["/usr/bin/sudo", "-u", "seq-user", "/usr/bin/srun", "--account=nsc",
                "--qos=high", "--partition=main"]
    SBATCH_GLSAI_ARGLIST=["/usr/bin/sudo", "-u", "seq-user", "/usr/bin/sbatch", "--account=nsc",
                "--qos=high", "--partition=main"]
    
    # When running on the command line we will be using a central user account,
    # so there's no need to sudo
    SRUN_OTHER_ARGLIST=["/usr/bin/srun", "--account=nsc", "--qos=high", "--partition=main"]

    # sbatch commands for "scheduler mode". Sudo mode is not supported.
    SBATCH_ARGLIST=["/usr/bin/sbatch", "--account=nsc", "--qos=high", "--partition=main"]
    
    # Args for jobs which mainly do I/O on the secondary storage, not processing
    SRUN_STORAGE_JOB_ARGS=["--nodelist=loki"]

    SQUEUE=["/usr/bin/squeue"]


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
        SECONDARY_STORAGE="/data/nsc.loki"         # location of demultiplexed files
        DELIVERY_DIR="/data/nsc.loki/delivery"     # used by prepare-delivery after QC
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


