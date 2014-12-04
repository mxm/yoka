#!/bin/bash

virtualenv -p /usr/bin/python2.7 env
source env/bin/activate
pip install -r requirements.txt
