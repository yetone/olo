#!/bin/bash

rm -rf ./test-reports
rm -rf ./coverage-reports
mkdir -p ./test-reports
mkdir -p ./coverage-reports

pip install virtualenv

virtualenv --clear VENV
. VENV/bin/activate

pip install -r requirements.txt
pip install -r test-requirements.txt

deactivate

exit $RET
