# Setup notes

Setting up the LIMS for use of these scripts, etc.


## Server

When slurm client is running on the LIMS servers (OUS): see nsc.py for two lines to add to `/etc/sudoers` to allow glsai user to run jobs as `seq-user`.


## Compute nodes

We have to save the credentials for the AI user somewhere accessible by the slurm jobs. On the closed network, we save it in the home directory of the `seq-user`.


## Protocol steps

### In ops interface:
Data processing processes!:
- Inputs: select analyte input, uncheck remove working status, except for final step of protocol
- Output Types: Uncheck Analyte outupt
Process UDFs:
- Job ID: Numeric type, uncheck Users can enter and modify values
- Job status: String type, and as above

Process outputs: 

* Copy run directory
    Copy run information which is not BCL and not fastq. 
* Demultiplexing (HiSeq)

