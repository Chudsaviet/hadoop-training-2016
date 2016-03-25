#!/bin/sh
set -e
set -x
mvn package
hdfs dfs -rm -r -skipTrash /apps/tk_hw1/ || true
hdfs dfs -mkdir /apps/tk_hw1/
hdfs dfs -chmod a+rwx /apps/tk_hw1/
hdfs dfs -rm -skipTrash /apps/tk_hw1/tk-homework1-1.0.jar || true
hdfs dfs -copyFromLocal target/tk-homework1-1.0-jar-with-dependencies.jar /apps/tk_hw1/tk-homework1-1.0.jar
hdfs dfs -copyFromLocal user.profile.tags.us.txt /apps/tk_hw1/user.profile.tags.us.txt || true
yarn jar target/tk-homework1-1.0.jar org.tk.hw1.Client hdfs:///apps/tk_hw1/user.profile.tags.us.txt hdfs:///apps/tk_hw1/result 10 hdfs:///apps/tk_hw1/tk-homework1-1.0.jar
hdfs dfs -ls -lah /apps/tk_hw1/result