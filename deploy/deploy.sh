#!/bin/bash
set -e

# Deployment script for OUS closed network
# Don't even attempt to be 

REPOS=pipeline genologics
INSTALLATIONS=/data/nsc.loki/automation /var/www/limsweb

# Deploy to installations from dev area
for installation in INSTALLATIONS
do
	for repo in REPOS
	do
		deploy_dir=$installation/$repo
		mv $deploy_dir $deploy_dir.2
		mkdir $deploy_dir
		source_dir=/data/nsc.loki/automation/dev/$repo
		pushd $source_dir > /dev/null
		git archive $1 | (pushd $deploy_dir; tar vx ; popd)
		chmod -R a+rX $deploy_dir
		chgrp -R nsc-seq $deploy_dir
		popd > /dev/null
		rm -rf $deploy_dir.2
	done
done
