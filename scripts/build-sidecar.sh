#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SCRIPT_DIR=$( dirname -- "$( readlink -f -- "$0"; )"; )
SIDECAR_REPO="${SIDECAR_REPO:-https://github.com/apache/cassandra-sidecar.git}"
SIDECAR_BRANCH="${SIDECAR_BRANCH:-trunk}"
SIDECAR_JAR_DIR="$(dirname "${SCRIPT_DIR}/")/dependencies"
SIDECAR_BUILD_DIR="${SIDECAR_JAR_DIR}/sidecar-build"
  # if [ ! -d "${SIDECAR_BUILD_DIR}" ] ; then
  #   git clone --depth 1 --single-branch --branch "${SIDECAR_BRANCH}" "${SIDECAR_REPO}" "${SIDECAR_BUILD_DIR}"
  #   git checkout "${SIDECAR_BRANCH}"
  # else
  #   pushd "${SIDECAR_BUILD_DIR}"
  #   git checkout "${SIDECAR_BRANCH}"
  #   git pull
  #   popd
  # fi

${SIDECAR_BUILD_DIR}/gradlew --project-dir=${SIDECAR_BUILD_DIR} -Pversion=1.0.0-analytics -Dmaven.repo.local=${SIDECAR_JAR_DIR} publishToMavenLocal
