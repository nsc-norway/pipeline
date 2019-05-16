#!/bin/bash
set -e
TAG=`date "+prod_%Y%m%d"$1`
./tag.sh $TAG
./deploy-cees.sh $TAG
