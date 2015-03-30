# NSC-specific configuration

from genologics.lims import *
from genologics.config import *
from ConfigParser import SafeConfigParser

# UDFs used in the LIMS for tracking automatic processing
AUTO_FLAG_UDF = "Automatic processing"
AUTO_FLOWCELL_UDF = "Automation lane groups"

JOB_ID_UDF = "Job ID"
JOB_STATUS_UDF = "Job status"

# UDFs for configuration and job steering
BASES_MASK_UDF = "Bases Mask"
THREADS_UDF = "Number of threads"
MISMATCHES_UDF = "Number of mismatches"
SOURCE_RUN_DIR_UDF = "Source run directory"
DEST_FASTQ_DIR_UDF = "Fastq output directory"
OTHER_OPTIONS_UDF = "Other options for configureBclToFastq"

# Other UDFs
# Tracking the location of fastq files
FILE_LOCATION = "Data location"


# Output files
CONFIGURE_LOG = "configureBclToFastq log"
MAKE_LOG = "make log"
DEMULTIPLEX_STATS_FILE = "Demultiplex_stats.htm"
FASTQ_OUTPUT = "{0} fastq"
LOG_DIR="/data/nsc.loki/automation/logs"

# Sequencing processes
SEQ_PROCESSES=[
        ('hiseq', 'Illumina Sequencing (Illumina SBS) 5.0')
        ]

# Auxiliary class to represent configuration
class StepSetup:
    def __init__(self, name, grouping, script = None):
        self.name = name
        self.grouping = grouping
        self.script = script

# Analysis after sequencing. List of protocols and per-protocol info.
# The top-level items are tuples of protocol name and lists of StepSetup 
# objects. The StepSetup objects represent a step in a protocol.
AUTOMATED_PROTOCOL_STEPS = [
        ("Demultiplexing and QC (HiSeq)",
            [
                StepSetup("Copy run directory (HiSeq)", "project"),
                StepSetup("Demultiplexing (HiSeq)", "project")
            ])
        ]

# System programs
RSYNC="/usr/bin/rsync"
MD5DEEP="/usr/bin/md5deep"
PDFLATEX="/usr/bin/pdflatex"

# Command line to run slurm
# ** OUS net: need to add this to sudoers to allow glsai to run as seq-user **
#glsai   ALL=(seq-user)  NOPASSWD:/usr/bin/sbatch
#Defaults:glsai          !requiretty
INVOKE_SBATCH_ARGLIST=["/usr/bin/sudo", "-u", "seq-user", "/usr/bin/sbatch",
        "-D", "/data/nsc.loki/automation/run"]


# Data processing programs
CONFIGURE_BCL_TO_FASTQ="/data/common/tools/nscbin/configureBclToFastq.pl"
MAKE="/usr/bin/make"
FASTQC=""

# Paths
PRIMARY_STORAGE = "/data/runScratch.boston"
SECONDARY_STORAGE="/data/nsc.loki"
DO_COPY_METADATA_FILES=True

# Template files
CUSTOMER_REPORT_TEMPLATE = "/data/nsc.loki/scripts/template/reportTemplate_indLane_v4.tex"
INTERNAL_HTML_TOP = "/data/nsc.loki/scripts/template/QC_NSC_report_template.html"
LOGO = "/data/nsc.loki/scripts/template/NSC_logo_original_RGB.tif"


# Group of files written (TODO: not currently used)
SET_GROUP='nsc-seq'

lims = Lims(BASEURI,USERNAME,PASSWORD)


config = SafeConfigParser({
    "slurm_script": "nsc-slurm.sh"
     })
config.read(['/etc/nsc.conf'])


# Todo: move into config file, or just use this as the config.


