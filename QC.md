# QC

Data quality analysis scripts: these scripts perform data quality analysis and preparations for delivery.

The dependency on Clarity LIMS is isolated in a few files, so that the QC scripts may be used without the LIMS.

* qc.py - main library for QC, reporting and delivery preparation
* setup-qc.py - trivial script which is run when starting the QC step in LIMS (compare: setup-hiseq-demultiplexing.py)
* qc-wrapper.py - interface to qc.py with optional LIMS integration
* qc-run.py - submits the QC job to slurm manually

