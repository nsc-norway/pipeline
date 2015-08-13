# NSC-specific configuration

from genologics.lims import *
import getpass
from ConfigParser import SafeConfigParser

# Configure prod or dev
TAG="dev"

if TAG == "prod":
    BASE_DIR = "/data/nsc.loki/automation"
elif TAG == "dev":
    BASE_DIR = "/data/nsc.loki/automation/dev"

# UDFs used in the LIMS for tracking automatic processing
AUTO_FLAG_UDF = "NSC Automatic processing"
AUTO_FLOWCELL_UDF = "Automation lane groups"

JOB_ID_UDF = "Job ID"
CURRENT_JOB_UDF = "Current job"
JOB_STATUS_UDF = "Job status"
JOB_STATE_CODE_UDF = "Job state code" #SUBMITTED,RUNNING,FAILED,COMPLETED,CANCELLED
ERROR_DETAILS_UDF = "Error details"

# UDFs for configuration and job steering (On process types)
RUN_ID_UDF = "Run ID"
THREADS_UDF = "Number of threads"
MISMATCHES_UDF = "Number of mismatches"
SOURCE_RUN_DIR_UDF = "Source run directory"
WORK_RUN_DIR_UDF = "Working run directory"
OTHER_OPTIONS_UDF = "Other options for bcl2fastq"
PROCESS_UNDETERMINED_UDF = "Process undetermined indexes"

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

# Output files
BCL2FASTQ_LOG = "bcl2fastq log"
FASTQC_LOG = "fastqc log"
FASTQ_OUTPUT = "{sample_name}"

# Values of the CURRENT_JOB_UDF
CJU_COPY_RUN = "Copy run"
CJU_DEMULTIPLEXING = "Demultiplexing"
CJU_SAVING_STATS = "Saving stats"
CJU_FASTQC = "FastQC"


# Sequencing processes
SEQ_PROCESSES=[
        ('hiseq', 'Illumina Sequencing (Illumina SBS) 5.0'),
        ('nextseq', 'NextSeq Run (NextSeq) 1.0'),
        ('miseq', 'MiSeq Run (MiSeq) 5.0')
        ]

# System programs
RSYNC="/usr/bin/rsync"
MD5DEEP="/usr/bin/md5deep"
PDFLATEX="/usr/bin/pdflatex"

# Command line to run slurm
# ** OUS net: need to add this to sudoers to allow glsai to run as seq-user **
# The first example below essentially allows glsai to run any command as seq-user,
# thus gaining access to the NSC storage volumes. I haven't found a good way to allow
# the script to set
# the resources etc., but restrict the command.
# TODO: modify sudo 
#glsai   ALL=(seq-user)  NOPASSWD:/usr/bin/sbatch
#Defaults:glsai          !requiretty
SCANCEL_ARGLIST=["/usr/bin/sudo", "-u", "seq-user", "/usr/bin/scancel"]
SRUN_ARGLIST=["/usr/bin/sudo", "-u", "seq-user", "/usr/bin/srun", 
            "--account=nsc", "--qos=high", "--partition=main", "--nodes=1"]

if TAG == "prod":
    WRAPPER_SCRIPT="/data/nsc.loki/automation/pipeline/slurm/ous-job.sh"
elif TAG == "dev":
    WRAPPER_SCRIPT="/data/nsc.loki/automation/dev/pipeline/slurm/ous-job.sh"

# Data processing programs
CONFIGURE_BCL_TO_FASTQ="/data/common/tools/nscbin/configureBclToFastq.pl"
MAKE="/usr/bin/make"
BCL2FASTQ2="/data/common/tools/nscbin/bcl2fastq"
FASTQC="/data/common/tools/nscbin/fastqc"
# Some programs don't have to be put here, because they are standard on all 
# machines: tar.

# Paths
PRIMARY_STORAGE = "/data/runScratch.boston"     # source data
if TAG == "prod":
    SECONDARY_STORAGE="/data/nsc.loki"         # location of demultiplexed files
    DELIVERY_DIR="/data/nsc.loki/delivery"     # used by prepare-delivery after QC
    DIAGNOSTICS_DELIVERY = "/data/diag/nscDelivery"
elif TAG == "dev":
    SECONDARY_STORAGE="/data/nsc.loki/test"    # location of demultiplexed files
    DELIVERY_DIR="/data/nsc.loki/test/delivery"# used by prepare-delivery after QC
    DIAGNOSTICS_DELIVERY = "/data/nsc.loki/test/diag"
    
LOG_DIR = BASE_DIR + "/logs"       # for slurm jobs
SCRATCH_DIR = BASE_DIR + "/run"    # not used
DO_COPY_METADATA_FILES=True

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


