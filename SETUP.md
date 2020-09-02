# Setup notes

There are two kinds of hosts to configure:

1. Pipeline server: Connects to LIMS and runs python/shell code in these scripts.
2. Compute node: Servers invoked to run tools. The compute node may be the same as the pipeline
server (CEES site) or may be a SLURM cluster (OUS site). If the compute node is the same,
the server must be configured according to both sections below.


## Pipeline executor server

This is the server running the scripts in this repository. Use the OS version: CentOS 7 or CentOS 8,
or RHEL, same versions.

The server setup on the OUS site is automated in the Ansible role `nsc-pipeline-host`. See readme file.

This section attempts to describe the setup in general.


### Dependencies (libraries)

Required yum packages:

- `python2-mock` (depends on `python2`, `python2-pip`, which are also needed)
- `python2-requests`
- `python2-jinja2`
- `texlive-latex`


Also, a package is needed from the current version of the Clarity repository. The current Clarity repo should be in `/etc/yum.repos.d/` on the Clarity LIMS server. Make sure to get the version corresponding to the running version of the LIMS. Copy the repo file to the same location on the pipeline runner server.

- `BaseSpaceLIMS-AutomationWorker`

Pip (use `sudo pip install`):

- `interop`

This is the Illumina data parsing library.

The scripts and LIMS-based automations expect a binary `nsc-python27` on the path. Create this
as a link to python2:
```
sudo ln -s python2 /usr/bin/nsc-python27
```


### Installing the scripts

The location where the scripts are stored should be writeable by relevant users who deploy new versions, and have the set-gid bit. For biolinux2 it's the nscdata group:
```
sudo mkdir -p /opt/nsc
sudo chgrp nscdata /opt/nsc
sudo chmod g+ws /opt/nsc
ls -ld /opt/nsc/
```
```
drwxrwsr-x. 2 root nscdata 4096 Sep  2 09:31 /opt/nsc/
```
The script location should match the paths used in the Step Automations configured in LIMS, under
Automations, for example "10. Copy run". The deployment scripts expect an old version of the 
scripts to exist, so first, create empty directories that will be deleted later:
```
mkdir -p /opt/nsc/{pipeline,genologics}
```

The scripts and genologics library can be deployed from a host that has checked out this git repo
and the genologics library from nsc-norway organisation in github. There is a deployment script, 
which is used to update to new versions.

```
(base) pcus307:git paalmbj$ ls -ld pipeline genologics
drwxr-xr-x  15 paalmbj  staff   480 Jun  8  2017 genologics
drwx------  37 paalmbj  staff  1184 Sep  2 09:36 pipeline
```
Go into `pipeline/deploy` and run the appropriate `auto-*.sh` script for the site. It will ask for
the SSH password to the server. Back on the pipeline server, make sure the group has R/W access to
the new files: (for example, biolinux2) `chmod -R g+rwX /opt/nsc/{pipeline,genologics}`.


### Security module

The module `secure.py` is not in git because it contains confidential information. It is stored in
a permanent location on the pipeline server. If installing a new pipeline server, then the module
must be copied manually from an old one. To find the location in which to place the file, see
the script `deploy/deploy-*.sh` for the location -- `ln -s .....secure.py` line.



### Install and configure Clarity automation worker

The package was already installed in the dependencies section. Configure it using the following
command:
```
sudo /opt/gls/clarity/automation_worker/node/bin/configure.sh
```
Enter the connection details for the LIMS (see installation record). The Channel should be the same
as is configured on the automations, such as the automation "10. Copy run". If new server, invent
one.

Start and enable the service:
```
sudo systemctl enable automation_worker
sudo systemctl start automation_worker
```
Monitor the logs and trigger an automation in LIMS. If it fails, also check wrapper.log in the same
dir.
```
sudo tail -f /opt/gls/clarity/automation_worker/node/logs/automatedinformatics.log
```


### Configuration files

The file `/etc/pipeline-site` determines the site configuration used by the scripts. The content
should be `ous` or `cees`, depending on the site.


#### A) Special servers

For biolinux2 and the OUS servers, the password for the Clarity user by name of apiuser is stored
in a text file. The location is referred to in `common/nsc.py`, `get_lims` function.


#### B) General server (not used)

The username and password of the API user must be stored in a file determined by the server type.
For unknown / default servers, there should be a file `.genologicsrc` in the home directory of the
`glsai` user.

```
[genologics]
BASEURI=https://<server>
USERNAME=apiuser
PASSWORD=<api-password>
```
User and password are the same as was configured for the automation_worker.



### Tools: MultiQC

MultiQC is run directly on the pipeline server (not "remote"). It can be somewhat challenging to
install it on RHEL 7 / Python 2.7, so we can use a singularity image. For this approach to work, you
need the singularity yum package installed.


### Test

Run:
```
python2 test_module.py
```
in the tests dir. It doesn't test much of the dependencies etc., but could catch some kinds of
errors.


### Pipeline server -- specifics

#### Network share -- biolinux2

For the purposes of these scripts, the LIMS server does not need to have access to the run storage.
Access is required for the sequencer integration services, which run on the LIMS server as the
glsjboss user. For this reason, we need to share the run storage with the LIMS server.

Export file on biolinux2:
`/etc/exports`:
```
/storage/nscdata/runsIllumina cees-lims.sequencing.uio.no(rw)
```
Enable and start the NFS server:
```
sudo systemctl enable nfs-server
sudo systemctl start nfs-server
```
The `rpcbind` service should also be running. It usually is. Enable access through the firewall.
```
sudo firewall-cmd --permanent --add-service=nfs
sudo firewall-cmd --permanent --add-service=rpcbind # recommended by others, but fails...
sudo firewall-cmd --permanent --add-service=mountd
sudo firewall-cmd --reload
```

User mapping: The UIDs of the glsjboss and glsai users are different. The only thing that needs to
match is that we make glsjboss a member of the nscdata group locally on the LIMS server.


## Compute node

The following packages are required on the compute nodes (may be the same as the pipeline server):

- `md5deep`
- `rsync`
- `java` (for FastQC only)

On CentOS / RHEL 8, `md5deep` is no longer available as a yum package (wtf?).


## Bioinformatics tools

See the file `common/nsc.py` for information about the expected path locations for these tools.
```
BCL2FASTQ2="/usr/local/bin/bcl2fastq"
FASTQC="/opt/FastQC/fastqc"
FASTDUP="/opt/nsc/bin/fastdup"
SUPRDUPR=False
```

These files should be readable from the compute nodes.

### bcl2fastq

Get from Illumina. For biolinux2 use RPM install, which will put the binary in 
/usr/local/bin

### FastQC

Download the FastQC zip file and unpack it somewhere.

FastQC depends on java. Install a java command to run it:
```
sudo yum install java
```

### suprDUPr (fastdup)

suprDUPr and fastdup are two names for the internal NSC duplicate detection tool. suprDUPr is the
newest version. One of SUPRDUPR or FASTDUP should be defined in the configuration file in nsc.py.

Download the tar.gz file from https://github.com/nsc-norway/suprDUPr/releases and unpack into a
directory.


## Location of scripts

Scripts may be stored in a directory accessible to the glsai user.

(OUS) The scripts are located in `/data/nsc.loki/automation/pipeline` and the supporting 
library by SciLifeLab in `/data/nsc.loki/automation/genologics`. The development versions
are in `/data/nsc.loki/automation/dev/`.


## Slurm integration

The commands to call slurm are configurable in the config file common/nsc.py. Normally, 
either sudo or ssh has to be used, as the glsai user can't submit jobs to slurm. See
comments in nsc.py for how to set up sudo. A user account with slurm access must be
available.

## Clarity LIMS Configuration

This is not documented yet.