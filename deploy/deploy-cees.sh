#!/bin/bash -e

# Deployment script for pipeline repository for CEES site.
# Deploys the pipeline and genologics repositories from the local filesystem to
# cees-lims and biolinux2.

# Usage: first tag the version to send (see tag.sh) then use this script.

# . deploy-cees.sh TAG

# Arguments: TAG = tag to send

for server in biolinux2.uio.no
do
	( pushd ../../genologics > /dev/null &&
	   git archive $1 &&
	   popd > /dev/null &&
	   pushd .. > /dev/null &&
	   git archive $1 && 
	   popd > /dev/null ) |
		ssh $server "/bin/bash -c '(pushd /opt/nsc > /dev/null &&
		(mv genologics genologics.2 || true) &&
		(mv pipeline pipeline.2 || true) &&
		mkdir genologics pipeline &&
		cd genologics &&
		tar x &&
		cd ../pipeline &&
		tar x && 
		sed -i \"s/^TAG=\\\"dev\\\"$/TAG=\\\"prod\\\"/\" common/nsc.py &&
		cd .. &&
		ln -s /opt/nsc/secure.py /opt/nsc/pipeline/common/secure.py &&
		rm -rf genologics.2 pipeline.2 )'"
done

