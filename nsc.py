# NSC-specific configuration

from genologics.lims import *
from genologics.config import *
from ConfigParser import SafeConfigParser

lims = Lims(BASEURI,USERNAME,PASSWORD)


config = SafeConfigParser({
    "slurm_script": "nsc-slurm.sh"
     })
config.read(['/etc/nsc.conf'])

slurm_script = config.get("nsc", "slurm_script")

