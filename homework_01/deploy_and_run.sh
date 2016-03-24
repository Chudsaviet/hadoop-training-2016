#!/bin/sh
set -e
mvn package
hdfs dfs -rm -skipTrash /apps/simple/simple-yarn-app-1.1.0.jar
hdfs dfs -copyFromLocal target/simple-yarn-app-1.1.0.jar /apps/simple/simple-yarn-app-1.1.0.jar
yarn jar target/simple-yarn-app-1.1.0.jar com.hortonworks.simpleyarnapp.Client hdfs:///apps/simple/user.profile.tags.us.txt hdfs:///apps/simple/result 1 hdfs:///apps/simple/simple-yarn-app-1.1.0.jar