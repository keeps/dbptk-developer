#!/bin/bash

set -ex

function deploy_to_artifactory() {
  echo "Deploy to artifactory"
  mvn $MAVEN_CLI_OPTS clean package deploy -Dmaven.test.skip=true -Pdefault
  # commented out to avoid breaking the build due to deployment failure (version will exist most of the time for this module)
  # mvn $MAVEN_CLI_OPTS deploy -Dmaven.test.skip=true -Pdbptk-bindings
}

if [ "$TRAVIS_BRANCH" == "master" ]; then
  echo "Logic for $TRAVIS_BRANCH branch"
  deploy_to_artifactory
  
fi
