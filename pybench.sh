#!/bin/bash

source env/bin/activate

#fab -f fabfile.py "$@"
python fabfile.py
