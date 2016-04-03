package org.tkorostelev.homework03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Point03ReducerTest {
    
    @Test
    public void testReducer()
            throws IOException, InterruptedException {

        List<IntWritable> input = Arrays.asList(new IntWritable(1), new IntWritable(10), new IntWritable(100));

        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new Point03Reducer())
                .withInput(new Text("123456"), input)
                .withOutput(new Text("123456"), new IntWritable(111))
                .runTest();
    }

}