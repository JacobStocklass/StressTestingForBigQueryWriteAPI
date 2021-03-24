#!/bin/bash

# Fail on any error.
set -e

echo Started building `date`

pushd StressTest
mvn install
popd

echo Waiting for building

wait

echo Done building `date`