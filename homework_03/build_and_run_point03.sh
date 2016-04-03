#!/bin/sh
set -x
set -e

# Test
echo "Running unit tests"
mvn -Dtest=Point03MapperTest test 2>p4_mapper_test.stderr >p4_mapper_test.stdout|| { echo "Mapper test fail"; exit 1; }
mvn -Dtest=Point03ReducerTest test 2>p4_reducer_test.stderr >p4_reducer_test.stdout|| { echo "Reducer test fail"; exit 1; }

# Build
echo "Building package"
time mvn package -Dmaven.test.skip=true 2>mvn_package.stderr >mvn_package.stdout

# Run
echo "Running job"
hdfs dfs -rm -r -skipTrash /apps/tk_hw3_p3/ || true
hdfs dfs -mkdir /apps/tk_hw3_p3/
hdfs dfs -chmod a+rwx /apps/tk_hw3_p3/
hdfs dfs -rm -skipTrash /apps/tk_hw3_p3/homework03-1.0.jar || true
hdfs dfs -copyFromLocal tags.txt.gz /apps/tk_hw3_p3/tags.txt.gz || true
time yarn jar target/homework03-1.0.jar org.tkorostelev.homework03.Point03 hdfs:///apps/tk_hw3_p3/tags.txt.gz hdfs:///dataset/stream.*.txt.gz hdfs:///apps/tk_hw3_p3/result
hdfs dfs -text /apps/tk_hw3_p3/result/part-r-00000 | head -10