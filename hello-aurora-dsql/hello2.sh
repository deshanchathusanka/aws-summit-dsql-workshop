#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Usage:  hello2.sh cluster_endpoint_url" 1>&2
    exit 1
fi

cd $HELLO_HOME
mvn package exec:java -Dexec.mainClass="software.amazon.dsql.HelloDSQL2" -Dexec.args="$1" --quiet
