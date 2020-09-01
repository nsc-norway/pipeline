# Setup notes

Setting up the demultiplexing / QC scripts: There are many moving pieces, possibly on different
servers. The workflow is managed by a cron job, and the commands are executed via the Clarity 
LIMS external command function.

## Pipeline executor server

This is the server running the scripts in this repository. Use the OS version: CentOS 7.

The server setup on the OUS site is automated in the Ansible role `nsc-pipeline-host`. See readme file.

This section attempts to describe the setup in general.

### Dependency packages

Required yum packages:

- `python-pip`
- `python2-mock`
- python2-requests ---? TBC

Also, a package is needed from the current version of the Clarity repository. The current Clarity repo should be in `/etc/yum.repos.d/` on the Clarity LIMS server. Make sure to get the version corresponding to the running version of the LIMS. Copy the repo file to the same location on the pipeline runner server.

- `BaseSpaceLIMS-AutomationWorker`

Pip (use pip install):

- Jinja2
- interop
requests
genologics (May use a separate checked out directory, not pip)


### Install and configure Clarity automation worker



### 

###


## Compute nodes

We have to save the credentials for the API user somewhere accessible by the slurm jobs.
On the closed network at NSC/OUS, we save it in the home directory of the `seq-user`.

    -rw-r-----. 1 seq-user seq-user 98 Mar 13 16:22 /home/seq-user/.genologicsrc

Content of .genologicsrc:

    [genologics]
    BASEURI=http://<server>
    USERNAME=apiuser
    PASSWORD=<api-password>


## Both LIMS servers and compute nodes
Install these yum packages (dependencies): 
python-argparse
python-requests


## Location of scripts

Scripts may be stored in a directory accessible to both the SLURM jobs and the glsai user.
At NSC/OUS this is done by making glsai a member of the nsc-seq group locally on the LIMS
servers (this may allow unauthorised users access to NSC internal volumes through the glsai
user, to be reconsidered). The scripts directory is then set to be readable by the nsc-seq
group. The configured script locations in Clarity then directly reference the scripts. 

(OUS) The scripts are located in `/data/nsc.loki/automation/pipeline` and the supporting 
library by SciLifeLab in `/data/nsc.loki/automation/genologics`. The development versions
are in `/data/nsc.loki/automation/dev/`.


## Slurm integration

The commands to call slurm are configurable in the config file common/nsc.py. Normally, 
either sudo or ssh has to be used, as the glsai user can't submit jobs to slurm. See
comments in nsc.py for how to set up sudo. A user account with slurm access must be
available.

## Clarity LIMS Configuration

### UDFs on existing Clarity objects
