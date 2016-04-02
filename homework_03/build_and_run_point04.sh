#!/bin/sh
set -x
set -e

mvn package

hdfs dfs -rm -r -skipTrash /apps/tk_hw3_p4/ || true
hdfs dfs -mkdir /apps/tk_hw3_p4/
hdfs dfs -chmod a+rwx /apps/tk_hw3_p4/
hdfs dfs -rm -skipTrash /apps/tk_hw3_p4/homework03-1.0.jar || true
yarn jar target/homework03-1.0.jar org.tkorostelev.homework03.Point04 hdfs:///dataset/stream.*.txt.gz hdfs:///apps/tk_hw3_p4/result
hdfs dfs -libjars ./target/homework03-1.0.jar -text /apps/tk_hw3_p4/result/part-r-00000 | head -10