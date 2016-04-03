#!/bin/sh
set -x
set -e

# Test
echo "Running unit tests"
mvn -Dtest=Point04MapperTest test 2>p4_mapper_test.stderr >p4_mapper_test.stdout|| { echo "Mapper test fail"; exit 1; }
mvn -Dtest=Point04ReducerTest test 2>p4_reducer_test.stderr >p4_reducer_test.stdout|| { echo "Reducer test fail"; exit 1; }

# Build
echo "Building package"
time mvn package -Dmaven.test.skip=true 2>mvn_package.stderr >mvn_package.stdout

# Run
echo "Running job"
hdfs dfs -rm -r -skipTrash /apps/tk_hw3_p4/ || true
hdfs dfs -mkdir /apps/tk_hw3_p4/
hdfs dfs -chmod a+rwx /apps/tk_hw3_p4/
hdfs dfs -rm -skipTrash /apps/tk_hw3_p4/homework03-1.0.jar || true
time yarn jar target/homework03-1.0.jar org.tkorostelev.homework03.Point04 hdfs:///dataset/stream.*.txt.gz hdfs:///apps/tk_hw3_p4/result
hdfs dfs -libjars ./target/homework03-1.0.jar -text /apps/tk_hw3_p4/result/part-r-00000 | head -10