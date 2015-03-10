# NSC-specific configuration

from genologics.lims import *
from genologics.config import *
from ConfigParser import SafeConfigParser

# UDFs used in the LIMS for tracking automatic processing
AUTO_FLAG_UDF = "Automatic processing"
AUTO_FLOWCELL_UDF = "Automation lane groups"

JOB_ID_UDF = "Job ID"
JOB_STATUS_UDF = "Job status"

BASES_MASK_UDF = "Bases mask"
THREADS_UDF = "Threads"
MISMATCHES_UDF = "Number of mismatches"
SOURCE_RUN_DIR_UDF = "Source run directory"
DEST_FASTQ_DIR_UDF = "Fastq output directory"

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
        ("Illumina SBS (HiSeq GAIIx) 5.0",
            [StepSetup("Bcl Conversion & Demultiplexing (Illumina SBS) 5.0", "project")])
        ]

# System programs
SBATCH="/usr/bin/sbatch"
RSYNC="/usr/bin/rsync"
LOG_DIR="/data/nsc.loki/robots/logs"

# Data processing programs
CONFIGURE_BCL_TO_FASTQ="/data/common/tools/nscbin/configureBclToFastq.pl"

# Paths
PRIMARY_STORAGE = "/data/runScratch.boston"
SECONDARY_STORAGE="/data/nsc.loki"

SET_GROUP='nsc-seq'

lims = Lims(BASEURI,USERNAME,PASSWORD)


config = SafeConfigParser({
    "slurm_script": "nsc-slurm.sh"
     })
config.read(['/etc/nsc.conf'])

#slurm_script = config.get("nsc", "slurm_script")


# Todo: move into config file, or just use this as the config.


