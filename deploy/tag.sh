#!/bin/bash
set -e

git tag $1
pushd ../../genologics > /dev/null
git tag $1
popd > /dev/null
echo "Tagged $1"

