#!/bin/sh
set -e
mvn package
rm /mnt/hdfs/apps/simple/simple-yarn-app-1.1.0.jar
cp target/simple-yarn-app-1.1.0.jar /mnt/hdfs/apps/simple/simple-yarn-app-1.1.0.jar || true
cp target/simple-yarn-app-1.1.0.jar /mnt/hdfs/apps/simple/simple-yarn-app-1.1.0.jar
yarn jar target/simple-yarn-app-1.1.0.jar com.hortonworks.simpleyarnapp.Client /bin/date 1 hdfs:///apps/simple/simple-yarn-app-1.1.0.jar
