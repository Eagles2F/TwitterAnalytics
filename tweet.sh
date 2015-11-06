#!/bin/bash
# description: tweets hbase server

cd /home/hadoop/TwitterAnalytics/

sudo env PATH="$PATH" CLASSPATH="$CLASSPATH" vertx run Server.java -instances 30
