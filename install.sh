#!/bin/bash

# fail on errors
set -e

virtualenv -p /usr/bin/python2.7 env
source env/bin/activate

pip install -r requirements.txt

if [ "$1" == "with_plotting" ] ; then
    pip install matplotlib
fi

echo "Installation completed successfully."
