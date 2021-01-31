#!/bin/bash
set -e
umask 002

# Deployment script for OUS closed network
# Don't even attempt to be site-independent.

# This script can be run from the directory in which it's located, as:
# ./deploy.sh TAG
echo $1

REPOS="pipeline genologics"
INSTALLATIONS="/data/runScratch.boston/scripts"

# Deploy to installations from dev area
for installation in $INSTALLATIONS
do
	for repo in $REPOS
	do
		deploy_dir=$installation/$repo
		mv $deploy_dir $deploy_dir.2 || true
		mkdir $deploy_dir
		source_dir=/data/runScratch.boston/scripts/dev/$repo
		pushd $source_dir > /dev/null
		git archive $1 | (pushd $deploy_dir; tar x ; popd)
		popd > /dev/null

		chgrp -R nsc-seq $deploy_dir

		rm -rf $deploy_dir.2
	done

	# pipeline specific
	deploy_dir=$installation/pipeline
	TEMP=`mktemp`
	sed 's/^TAG="dev"$/TAG="prod"/' $deploy_dir/common/nsc.py | sed "s/^VERSION=.*/VERSION='$1'/" > $TEMP
	cp $TEMP $deploy_dir/common/nsc.py
	rm $TEMP
done

ln -s /data/runScratch.boston/scripts/etc/secure.py /data/runScratch.boston/scripts/pipeline/common/secure.py

