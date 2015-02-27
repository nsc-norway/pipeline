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

