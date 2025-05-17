#!/bin/bash
cd $HELLO_HOME
mvn package exec:java -Dexec.mainClass="software.amazon.dsql.HelloDSQL1" --quiet
