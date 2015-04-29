# NSC-specific configuration

from genologics.lims import *
from genologics.config import *
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
JOB_STATUS_UDF = "Job status"

# UDFs for configuration and job steering (On process types)
BASES_MASK_UDF = "Bases Mask"
THREADS_UDF = "Number of threads"
MISMATCHES_UDF = "Number of mismatches"
SOURCE_RUN_DIR_UDF = "Source run directory"
DEST_FASTQ_DIR_UDF = "Fastq output directory"
NS_OUTPUT_RUN_DIR_UDF = "Output run directory" #NextSeq
COPY_MISEQ_DEST_UDF = "Copy run to"
OTHER_OPTIONS_UDF = "Other options for configureBclToFastq"
NS_OTHER_OPTIONS_UDF = "Other options for bcl2fastq"

# Other UDFs 
# On Analyte
LANE_UNDETERMINED_UDF = "NSC % Undetermined Indices (PF)"
# On Project
DELIVERY_METHOD_UDF = "Delivery method"
PROJECT_TYPE_UDF = "Project type"


# Output files
CONFIGURE_LOG = "configureBclToFastq log"
MAKE_LOG = "make log"
BCL2FASTQ_LOG = "bcl2fastq log"
HISEQ_FASTQ_OUTPUT = "{0} R{1} fastq"
NEXTSEQ_FASTQ_OUTPUT = "{0} R{1} fastq"

# Sequencing processes
SEQ_PROCESSES=[
        ('hiseq', 'Illumina Sequencing (Illumina SBS) 5.0'),
#        ('nextseq', 'Something'),
        ('miseq', 'MiSeq Run (MiSeq) 5.0')
        ]

# Auxiliary class to represent configuration
class StepSetup:
    def __init__(self, name, grouping, script = None):
        """name is the name of the process type

        grouping can be: project, flowcell. Determines which
        lanes can be processed together. flowcell requires all
        lanes together, project only includes lanes from the same
        project in any given job. TODO: do we need "lane", maybe
        for CEES/Abel, to make more slurm jobs?
        """
        self.name = name
        self.grouping = grouping
        self.script = script

# Analysis after sequencing. List of protocols and per-protocol info.
# The top-level items are tuples of protocol name and lists of StepSetup 
# objects. The StepSetup objects represent a step in a protocol (see above).
AUTOMATED_PROTOCOL_STEPS = [
            ("Illumina SBS (HiSeq GAIIx) 5.0",
            [
                StepSetup("NSC Demultiplexing (HiSeq)", "project", "Submit demultiplexing job"),
                StepSetup("NSC Data Quality Reporting (HiSeq)", "project", "Submit QC job"),
                StepSetup("NSC Prepare for delivery", "project", "Submit delivery job")
            ]),
            ("Illumina SBS (MiSeq) 5.0",
            [
                StepSetup("NSC Copy MiSeq Run", "flowcell", "Copy MiSeq Run"),
                StepSetup("NSC Data Quality Reporting (Mi/NextSeq)", "flowcell", "Submit QC job")
            ]),
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
BCL2FASTQ2="/data/common/tools/nscbin/bcl2fastq"
FASTQC="/data/common/tools/nscbin/fastqc"
# Some programs don't have to be put here, because they are standard on all 
# machines: tar.

# Paths
PRIMARY_STORAGE = "/data/runScratch.boston"     # source data
if TAG == "prod":
    SECONDARY_STORAGE="/data/nsc.loki"         # location of demultiplexed files
    DELIVERY_DIR="/data/nsc.loki/delivery"     # used by prepare-delivery after QC
elif TAG == "dev":
    SECONDARY_STORAGE="/data/nsc.loki/test"    # location of demultiplexed files
    DELIVERY_DIR="/data/nsc.loki/test/delivery"# used by prepare-delivery after QC
LOG_DIR = BASE_DIR + "/logs"       # for slurm jobs
SCRATCH_DIR = BASE_DIR + "/run"    # not used
DO_COPY_METADATA_FILES=True


# Group of files written (TODO: not currently used)
SET_GROUP='nsc-seq'

lims = Lims(BASEURI,USERNAME,PASSWORD)


config = SafeConfigParser({
    "slurm_script": "nsc-slurm.sh"
     })
config.read(['/etc/nsc.conf'])


# Todo: move into config file, or just use this as the config.


