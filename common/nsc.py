# NSC-specific configuration

from genologics.lims import *
import getpass
from ConfigParser import SafeConfigParser

# Configure prod or dev
TAG="dev"

SITE="ous"

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

# Other UDFs 
# On process types
BCL2FASTQ_VERSION_UDF = "bcl2fastq version"
# On Analyte
LANE_UNDETERMINED_UDF = "NSC % Undetermined Indices (PF)"
# On Project
DELIVERY_METHOD_UDF = "Delivery method"
PROJECT_TYPE_UDF = "Project type"
# On Container
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
    BCL2FASTQ2=""
    FASTQC="/opt/FastQC/fastqc"


    REMOTE_MODE = "local"

    SSH_ARGLIST = ["/usr/bin/ssh", "biolinux2.uio.no"]

elif SITE == "ous":
    # Data processing/analysis programs
    #BCL2FASTQ2="/data/common/tools/nscbin/bcl2fastq"
    BCL2FASTQ2="/data/common/tools/bcl2fastq/bcl2fastq2-v2.17.1.14/nscinstallbin/bin/bcl2fastq"
    FASTQC="/data/common/tools/nscbin/fastqc"


    REMOTE_MODE = "srun"

    # Command line to run slurm
    # ** OUS net: need to add this to sudoers to allow glsai to run as seq-user **
    # The first example below essentially allows glsai to run any command as seq-user,
    # thus gaining access to the NSC storage volumes. I haven't found a good way to allow
    # the script to set
    # the resources etc., but restrict the command.
    # TODO: modify sudo 
    #glsai   ALL=(seq-user)  NOPASSWD:/usr/bin/sbatch
    #Defaults:glsai          !requiretty
    SRUN_GLSAI_ARGLIST=["/usr/bin/sudo", "-u", "seq-user", "/usr/bin/srun", 
                "--account=nsc", "--qos=high", "--partition=main", "--nodes=1"]
    
    # When running on the command line we will be using a central user account,
    # so there's no need to sudo
    SRUN_OTHER_ARGLIST=["/usr/bin/srun", "--account=nsc", "--qos=high",
                                        "--partition=main", "--nodes=1"]
    
    # Args for jobs which mainly do I/O on the secondary storage, not processing
    SRUN_STORAGE_JOB_ARGS=["--nodelist=loki"]


### Site and phase (TAG) dependent configuration ###

# Paths
if SITE == "cees":
    PRIMARY_STORAGE = "/storage/nscdata/runsIllumina"
    if TAG == "prod":
        SECONDARY_STORAGE="/storage/nscdata/runsIllumina"
        #DELIVERY_DIR="/data/nsc.loki/delivery"     # used by prepare-delivery after QC
        BASE_DIR = "/opt/nsc/pipeline"

    elif TAG == "dev":
        # TODO: dev environment on UiO net?
        pass

elif SITE == "ous":
    PRIMARY_STORAGE = "/data/runScratch.boston"     # source data
    if TAG == "prod":
        SECONDARY_STORAGE="/data/nsc.loki"         # location of demultiplexed files
        DELIVERY_DIR="/data/nsc.loki/delivery"     # used by prepare-delivery after QC
        DIAGNOSTICS_DELIVERY = "/data/diag/nscDelivery"
        BASE_DIR = "/data/nsc.loki/automation"
        LOG_DIR = "/data/nsc.loki/automation/logs" # logs for copy job (10_... script used at OUS)

    elif TAG == "dev":
        SECONDARY_STORAGE="/data/nsc.loki/test"    # location of demultiplexed files
        DELIVERY_DIR="/data/nsc.loki/test/delivery"# used by prepare-delivery after QC
        DIAGNOSTICS_DELIVERY = "/data/nsc.loki/test/diag"
        LOG_DIR = "/data/nsc.loki/automation/dev/logs"


# Configure LIMS access (should be cleaned up)
if TAG == "dev":
    from genologics.config import *
    lims = Lims(BASEURI,USERNAME,PASSWORD)
elif TAG == "prod":
    if getpass.getuser() == "seq-user":
        pw_file = "/data/nsc.loki/automation/etc/seq-user/apiuser-password.txt"
    elif getpass.getuser() == "glsai":
        pw_file = "/opt/gls/clarity/users/glsai/apiuser-password.txt"
    elif getpass.getuser() == "limsweb":
        pw_file = "/var/www/limsweb/private/password"
    else:
        pw_file = None
    
    if pw_file: # make sure this can load w/o pw file, for non-lims tasks
        lims = Lims(
                "http://ous-lims.ous.nsc.local:8080",
                "apiuser",
                open(pw_file).read().strip()
                )


