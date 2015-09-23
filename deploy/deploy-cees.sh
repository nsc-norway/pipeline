#!/bin/bash -e

# Deployment script for pipeline repository for CEES site.
# Deploys the pipeline and genologics repositories from the local filesystem to
# cees-lims.

# Usage: first tag the version to send (see tag.sh) then use this script.

# Arguments: tag to send

( pushd ../../genologics > /dev/null &&
   git archive $1 &&
   popd > /dev/null &&
   pushd .. > /dev/null &&
   git archive $1 && 
   popd > /dev/null ) |
   	ssh biolinux2.uio.no "/bin/bash -c '(pushd /opt/nsc > /dev/null &&
	mv genologics genologics.2 &&
	mv pipeline pipeline.2 &&
	mkdir genologics pipeline &&
	cd genologics &&
	tar x &&
	cd ../pipeline &&
	tar x && 
	cd .. &&
	rm -rf genologics.2 pipeline.2 )'"

