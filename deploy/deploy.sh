#!/bin/bash
set -e

REPOS=pipeline genologics
DIR=1
for repo in REPOS
do
	mv /data/nsc.loki/automation/$repo /data/nsc.loki/automation/$repo.2
	mkdir /data/nsc.loki/automation/$repo
	git archive $1 | (pushd 
done
