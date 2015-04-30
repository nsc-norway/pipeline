# Setup notes

Setting up the LIMS for use of these scripts, etc.


## Server

When slurm client is running on the LIMS servers (OUS): see nsc.py for two lines to add
to `/etc/sudoers` to allow glsai user to run jobs as `seq-user`.


## Compute nodes

We have to save the credentials for the API user somewhere accessible by the slurm jobs.
On the closed network at NSC/OUS, we save it in the home directory of the `seq-user`.

    -rw-r-----. 1 seq-user seq-user 98 Mar 13 16:22 /home/seq-user/.genologicsrc

Content of .genologicsrc:

    [genologics]
    BASEURI=http://<server>:8080
    USERNAME=apiuser
    PASSWORD=<api-password>


## Location of scripts

Scripts may be stored in a directory accessible to both the SLURM jobs and the glsai user.
At NSC/OUS this is done by making glsai a member of the nsc-seq group locally on the LIMS
servers (this may allow unauthorised users access to NSC internal volumes through the glsai
user, to be reconsidered). The scripts directory is then set to be readable by the nsc-seq
group. The configured script locations in Clarity then directly reference the scripts. 

(OUS) The scripts are located in `/data/nsc.loki/automation/pipeline` and the supporting 
library by SciLifeLab in `/data/nsc.loki/automation/genologics`. The development versions
are in `/data/nsc.loki/automation/dev/`.


## Clarity LIMS Configuration


### Process types





## Cron job

Pass

