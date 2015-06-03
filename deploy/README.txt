#### Deployment scripts ####

LIMS data processing package, deployment procedure.

This file describes site-specific deployment procedure for the code and associated
files. For configuration of the LIMS itself, see the SETUP files.

The deployment always includes both the "pipeline" and the "genologics" repositories.


### OUS deployment ###

The production code and template files are stored in two places:
/data/nsc.loki/automation/{pipeline,genologics}
/var/www/limsweb/{pipeline,genologics}

These are essentially identical copies. The former is used by the LIMS integration and
automation logic. The latter is used by the "overview" web interface. (it is necessary to
have two parallel installations because the web server cannot access the "pipeline" 
directory)


## Procedure ##

1. Tag the production release of pipeline and genologics with the same tag.

   You may use a previously tagged commit, in that case skip to step 2.

   Make sure the version to be deployed is checked out, and that all changes
   are committed. Then run:

   $ ./tag.sh <TAG>

   Use a tag prod_<DATE>, e.g. prod_20150603.

2. Deploy it.

   $ ./deploy.sh <TAG>



