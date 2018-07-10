#!/bin/bash

set -o pipefail

find olo -name '*.so' -delete
find olo -name '*.c' -delete

rm -rf pylint.out
python -m pylint.lint olo --errors-only --rcfile=./.pylintrc | tee pylint.out
