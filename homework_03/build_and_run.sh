#!/bin/sh
set -x
set -e

mvn package

hdfs dfs -rm -r -skipTrash /apps/tk_hw3/ || true
hdfs dfs -mkdir /apps/tk_hw3/
hdfs dfs -chmod a+rwx /apps/tk_hw3/
hdfs dfs -rm -skipTrash /apps/tk_hw3/homework03-1.0.jar || true
hdfs dfs -copyFromLocal tags.txt.gz /apps/tk_hw3/tags.txt.gz || true
yarn jar target/homework03-1.0.jar org.tkorostelev.homework03.Point03 hdfs:///apps/tk_hw3/tags.txt.gz hdfs:///dataset/stream.20130611-af.txt.gz hdfs:///apps/tk_hw3/result
hdfs dfs -ls /apps/tk_hw3/result