# Setup notes

Setting up the LIMS for use of these scripts, etc.


For the overview page, see the SETUP instruction in the monitor/ directory, in addition
to the ones given here.


## Server

When slurm client is running on the LIMS servers (OUS): see nsc.py for two lines to add
to `/etc/sudoers` to allow glsai user to run jobs as `seq-user`.

Add the glsai to UNIX groups if necessary. For OUS, add it to the nsc-seq group, so it
can read the scripts. Use sudo vigr to add this to /etc/group:
nsc-seq:x:163877:glsai
and use sudo vigr -s to add this to /etc/gshadow:
nsc-seq:!::glsai


## Compute nodes

We have to save the credentials for the API user somewhere accessible by the slurm jobs.
On the closed network at NSC/OUS, we save it in the home directory of the `seq-user`.

    -rw-r-----. 1 seq-user seq-user 98 Mar 13 16:22 /home/seq-user/.genologicsrc

Content of .genologicsrc:

    [genologics]
    BASEURI=http://<server>:8080
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

#### Analyte
 - `NSC % Undetermined Indices (PF)` -- Numeric, 1 decimal place. Information field for the undetermined percentage on a lane.

#### Container
 - `Automation lane groups` -- Single line text. List of lanes which can be run together, used by auto.py.
 - `Recently completed` -- Single line text. Date at which the processing of this run was completed. Helper field for overview page.
   [ ] Shown in tables
   [ ] User can enter values via GUI
   TODO <<< Figure this one out

#### Project
 - `Delivery method` -- Single line text. For the prepare-delivery.py script. Should be one of User HDD, New HDD, Norstore, Transfer to diagnostics.

#### Genologics-provided process types

 - `NSC Automatic processing` -- Check box. Enables automatic processing for all projects in a given sequencing run. Used by auto.py.
   - Process types: Illumina Sequencing (Illumina SBS) 5.0, MiSeq Run (MiSeq) 5.0, NextSeq Run (NextSeq) 1.0

### New process types

Only non-default values are given below. The default settings are: 
 - Input types:    Analyte input: enabled, remove working status not checked. ResultFile 
                   input: disabled.
 - Output types:   None enabled.
 - Output details: empty.
 - Output generation: empty. 
 - Attributes: irrelevant.
 - External programs: Unchecked, None. 

Implicit settings for external programs: 

If an external program is required, please check the box at the top of the screeen. For
each external program, the Channel Name should always be limsserver.

These UDFs are present on all process types which submit slurm jobs: `Job ID`: Numeric, uncheck users can enter values, `Job status`: single line text, uncheck users can enter values, `Job state code`: single line text, uncheck users can enter values.

The "Cancel job" command is the same on all slurm-based process types: Name: Cancel job. Command: /usr/bin/python /data/nsc.loki/automation/pipeline/kill-slurm.py {processLuid}.

#### Copy MiSeq Run
 - Name: NSC Copy MiSeq Run
 - Output types: none.
 - External programs: 
   - Name: Copy MiSeq Run. Command line: /usr/bin/python /data/nsc.loki/automation/pipeline/epp-submit-slurm.py --pid={processLuid} --time=1:00:00 --threads=1 --mem=512 --jobname={processLuid} /data/nsc.loki/automation/pipeline/copy-miseq.py {processLuid}
   - Cancel job command.
 - UDFs: Slurm UDFs.

#### NSC Data Quality Reporting (HiSeq)
 - Name: NSC Data Quality Reporting (HiSeq)
 - Output types: none.
 - External programs: 
  - Name: Submit QC job. Command line: /usr/bin/python /data/nsc.loki/automation/pipeline/epp-submit-slurm.py --pid={processLuid} --time=20:00:00 --threads={udf:Number of threads} --mem=1024 --thread-mem=512 --jobname={processLuid} /data/nsc.loki/automation/pipeline/run-qc-hiseq.py --pid={processLuid}
   - Cancel job command.
 - UDFs: Slurm UDFs, Number of threads: numeric, process undetermined indexes: chekcbox.

#### NSC Data Quality Reporting
 - Name: NSC Data Quality Reporting (Mi/NextSeq)
 - Output types: none.
 - External programs:
   - Name: Submit QC job. Command: /usr/bin/python /data/nsc.loki/automation/pipeline/epp-submit-slurm.py --pid={processLuid} --time=20:00:00 --threads={udf:Number of threads} --mem=1024 --thread-mem=512 --jobname={processLuid} /data/nsc.loki/automation/pipeline/run-qc.py --pid={processLuid}
   - Slurm cancel job.
 - UDFs: Slurm UDFs, Process undetermined indexes: checkbox, Number of threads: Numeric.


#### NSC Demultiplexing (HiSeq)
 - Name: NSC Demultiplexing (HiSeq) 
 - Output types: Disable per input. Enable Shared outputs. Enable Outputs per Reagent Label.
 - Output details: Select output per reagent label, and check:
   - Yield PF (Gb), %PF, # Reads, % of Raw Clusters Per Lane, % Perfect Index Reads, % One Mismatch Reads (Index), % Bases >= Q30, Ave Q Score.
 - Output generation:
   - Shared output: ResultFile: Fixed number=4, Name= {LIST:SampleSheet csv,configureBclToFastq log,make log,Demultiplex_stats.htm}
   - O. per Input per reagent: Fixed number=2, Name={LIST:{SubmittedSampleName} R1 fastq,{SubmittedSampleName} R2 fastq}
 - External programs:
   - Name: Submit demultiplexing job. Command: /usr/bin/python /data/nsc.loki/automation/pipeline/epp-submit-slurm.py --pid={processLuid} --time=20:00:00 --threads={udf:Number of threads} --mem=2048 --thread-mem=512 --jobname={processLuid} /data/nsc.loki/automation/pipeline/demultiplex-hiseq.py {processLuid}
   - Name: Set demultiplexing options. Command: /usr/bin/python /data/nsc.loki/automation/pipeline/setup-hiseq-demultiplexing.py {processLuid} {compoundOutputFileLuid0}
   - Cancel job command.
 - UDFs: Slurm UDFs, Bases Mask: text, Number of mismatches: numeric; use first preset as default; add 1 as preset, Source run directory: text, Fastq output directory: text, Number of threads: numeric, Other options for configureBclToFastq: text.


#### NSC Prepare for delivery
 - Name: NSC Prepare for delivery
 - Output types: none.
 - External programs:
   - Name: Submit delivery job. Command: /usr/bin/python /data/nsc.loki/automation/pipeline/epp-submit-slurm.py --pid={processLuid} --time=20:00:00 --threads=1 --mem=1024 --jobname={processLuid} /data/nsc.loki/automation/pipeline/prepare-delivery.py {processLuid}
   - Cancel job command.
 - UDFs: Slurm UDFs.


#### NSC Demultiplexing (NextSeq)
 - Name: NSC Demultiplexing (NextSeq)
 - Output types: Disable per input. Enable Shared outputs. Enable Outputs per Reagent Label.
 - Output details: Select output per reagent label, and check:
   - Yield PF (Gb), %PF, # Reads, % of Raw Clusters Per Lane, % Perfect Index Reads, % One Mismatch Reads (Index), % Bases >= Q30, Ave Q Score.
 - Output generation:
   - Shared output: ResultFile: Fixed number=2, Name={LIST:SampleSheet csv,bcl2fastq log}
   - O. per Input per reagent: Fixed number=2, Name={LIST:{SubmittedSampleName} R1 fastq,{SubmittedSampleName} R2 fastq}
 - External programs:
   - Name: Submit demultiplexing job. Command: /usr/bin/python /data/nsc.loki/automation/pipeline/epp-submit-slurm.py --pid={processLuid} --time=20:00:00 --threads={udf:Number of threads} --mem=2048 --thread-mem=512 --jobname={processLuid} /data/nsc.loki/automation/pipeline/demultiplex-nextseq.py {processLuid}
   - Name: Set demultiplexing options. Command: /usr/bin/python /data/nsc.loki/automation/dev/pipeline/setup-nextseq-demultiplexing.py {processLuid} {compoundOutputFileLuid0}
   - Cancel job command.
  - UDFs: Slurm UDFs. Number of threads: Numeric; check "Use first preset value as default"; add a preset value 10, Source run directory: text, Fastq output directory: text, Other options for bcl2fastq: text, Bases Mask: text.


#### NSC Finalize run
Process to 
 - Name: NSC Finalize run
 - Output types: Disable "per input".(none)
 - External programs:
   - Name: Submit. Command: /usr/bin/python /data/nsc.loki/automation/pipeline/epp-submit-slurm.py --pid={processLuid} --time=00:10 --threads=1 --mem=128 --jobname={processLuid} /data/nsc.loki/automation/pipeline/finalize-run.py {processLuid} 
 - UDFs: Slurm UDFs.
 

### Protocols
Create these protocols. The configuration is listed in line for each protocol step. On all
protocol steps corresponding to slurm jobs, with slurm UDFs, the field "Job state code" 
should be unchecked in the Record Details section.


#### NSC Data processing for HiSeq
Type: Data Analysis
Capacity: 20 (approx. 20 lanes)
Add the following protocol steps:
 - NSC Demultiplexing (HiSeq)
   Config: 
    - Automation: Change Set demultiplexing options, set Auomatically initiated, on Record Details, when screen is entered
    - Record Details: In Sample Details section, add / remove so the Selected Fields from the Sample measurement section are: # Reads, % of Raw Clusters Per Lane, % Bases >= Q30. Leave others as default.
 - NSC Data Quality Reporting (HiSeq)
 - NSC Prepare for delivery


#### NSC Data processing for NextSeq
Type: Data Analysis
Capacity: 5
Add these protocol steps: 
 - NSC Demultiplexing (NextSeq)
   Config: 
    - Automation: Change Set demultiplexing options, set Auomatically initiated, on Record Details, when screen is entered
    - Record Details: In Sample Details section, add / remove so the Selected Fields from the Sample measurement section are: # Reads, % of Raw Clusters Per Lane, % Bases >= Q30. Leave others as default.
 - NSC Data Quality Reporting (Mi/NextSeq)
 - NSC Prepare for delivery


#### NSC Data processing for MiSeq
Type: Data Analysis
Capacity: 5
 - NSC Copy MiSeq Run
 - NSC Data Quality Reporting (Mi/NextSeq)
 - NSC Prepare for delivery







## Cron job

Pass

