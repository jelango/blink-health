#!/bin/sh
set -eu

rm -rf venv
virtualenv-3.6 venv

set +u
source venv/bin/activate
set -u

pip install --upgrade --ignore-installed pip setuptools
pip install pybuilder==0.11.17

pyb install_build_dependencies
pyb install_runtime_dependencies
