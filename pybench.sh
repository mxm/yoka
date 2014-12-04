#!/bin/bash

source env/bin/activate

python runs.py 2>&1 | tee output.log
