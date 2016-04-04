#!/bin/sh
set -x
set -e

# Test
# echo "Running unit tests"
# mvn -Dtest=Point01MapperTest test 2>p1_mapper_test.stderr >p1_mapper_test.stdout|| { echo "Mapper test fail"; exit 1; }

# Build
echo "Building package"
time mvn package -Dmaven.test.skip=true 2>mvn_package.stderr >mvn_package.stdout

# Run
echo "Running job"
hdfs dfs -rm -r -skipTrash /apps/tk_hw4_p1/ || true
hdfs dfs -mkdir /apps/tk_hw4_p1/
hdfs dfs -chmod a+rwx /apps/tk_hw4_p1/
hdfs dfs -rm -skipTrash /apps/tk_hw4_p1/homework04-1.0.jar || true
time yarn jar target/homework04-1.0.jar org.tkorostelev.homework04.Point01 hdfs:///dataset/stream.20130611-ae.txt.gz hdfs:///apps/tk_hw4_p1/result
hdfs dfs -libjars ./target/homework04-1.0.jar -text /apps/tk_hw4_p1/result/part-r-00000.bz2 | head -10