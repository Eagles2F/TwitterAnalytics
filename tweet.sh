#!/bin/bash
# description: tweets hbase server

cd /home/ubuntu/TwitterAnalytics/

sudo env PATH="$PATH" CLASSPATH="$CLASSPATH" vertx run Server.java
