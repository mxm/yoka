#!/bin/bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

rotateLogFile() {
    log=$1;
    num=9
    if [ -f "$log" -a "$num" -gt 0 ]; then
        while [ $num -gt 1 ]; do
            prev=`expr $num - 1`
            [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
            num=$prev
        done
        mv "$log" "$log.$num";
    fi
}

printUsage(){
    echo "usage: ./pybench.sh <run_name>"
}

printRunNames(){
    echo "Run names found:"
    for path in `ls runs/*.py` ; do
        name=`basename $path .py`
        echo "    $name"
    done
}

runFile="runs/$1.py"

if [ "$#" -ne 1 ]; then
    printUsage
    echo
    printRunNames
    exit 1
elif [ -e $runFile ] ; then
    source env/bin/activate

    logFile="runs/$1.stdout.log"
    rotateLogFile "$logFile"

    export PYTHONPATH="$PYTHONPATH:./"
    python "$runFile" 2>&1 | tee "$logFile"
else
    printUsage
    echo
    echo "$1 is not a valid run name."
    echo
    printRunNames
    exit 1
fi
