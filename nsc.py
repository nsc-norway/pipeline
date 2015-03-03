# NSC-specific configuration

from genologics.lims import *
from genologics.config import *
from ConfigParser import SafeConfigParser

AUTO_FLAG_UDF = "Automatic processing"
AUTO_POOL_UDF = "Automatic processing group"

SEQ_PROCESSES={
        'hiseq': 'Illumina Sequencing (Illumina SBS) 5.0',
        'miseq': 'Illumina Sequencing'
        }

MONITOR_STEP_IDS = {
        "24-001": "demultiplexing_hiseq"
        }


lims = Lims(BASEURI,USERNAME,PASSWORD)


config = SafeConfigParser({
    "slurm_script": "nsc-slurm.sh"
     })
config.read(['/etc/nsc.conf'])

#slurm_script = config.get("nsc", "slurm_script")

class StepSetup:
    def __init__(self, name, grouping, script = None):
        self.name = name
        self.grouping = grouping
        self.script = script

# Todo: move into config file, or just use this as the config.
automated_protocol_steps = [
        ("Illumina SBS (HiSeq GAIIx) 5.0",
            [StepSetup("Bcl Conversion & Demultiplexing (Illumina SBS) 5.0", "project")])
        ]


