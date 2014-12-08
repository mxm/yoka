#!/bin/bash

source env/bin/activate

if [ "$#" -ne 1 ]; then
    echo "usage: ./pybench.sh <run_file>"
    exit 1
fi

python "$1" 2>&1 | tee "$1.output.log"
