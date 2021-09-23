#!/bin/bash
#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
# the specific language governing permissions and limitations under the License.
#

#
# This assumes all of the OS-level configuration has been completed and git repo has already been cloned
#
# This script should be run from the repo's deployment directory
# cd deployment
# ./run-unit-tests.sh
#

[ "$DEBUG" == 'true' ] && set -x
set -e

# Get reference for all important folders
template_dir="$PWD"
source_dir="$(cd $template_dir/../source; pwd -P)"
root_dir="$template_dir/.."

echo "------------------------------------------------------------------------------"
echo "[Init] Clean old folders"
echo "------------------------------------------------------------------------------"

cd $root_dir
if [ -d ".venv" ]; then
  rm -rf ".venv"
fi

echo "------------------------------------------------------------------------------"
echo "[Env] Create virtual environment and install dependencies"
echo "------------------------------------------------------------------------------"

virtualenv .venv
source .venv/bin/activate

cd $source_dir
pip install -r $source_dir/requirements-dev.txt
cd -

echo "------------------------------------------------------------------------------"
echo "[Test] Run pytest with coverage"
echo "------------------------------------------------------------------------------"
cd $source_dir
# setup coverage report path
coverage_report_path=$source_dir/tests/coverage-reports/source.coverage.xml
echo "coverage report path set to $coverage_report_path"

pytest --cov --cov-report=term-missing --cov-report "xml:$coverage_report_path"

# The pytest --cov with its parameters and .coveragerc generates a xml cov-report with `coverage/sources` list
# with absolute path for the source directories. To avoid dependencies of tools (such as SonarQube) on different
# absolute paths for source directories, this substitution is used to convert each absolute source directory
# path to the corresponding project relative path. The $source_dir holds the absolute path for source directory.
sed -i -e "s,<source>$source_dir,<source>source,g" $coverage_report_path

# deactivate the virtual environment
deactivate

cd $template_dir

