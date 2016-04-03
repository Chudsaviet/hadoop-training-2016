package org.tkorostelev.homework03;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Point04ReducerTest {

    @Test
    public void testReducer()
            throws IOException, InterruptedException {
        Point04Writable record1 = new Point04Writable(new IntWritable(2), new DoubleWritable(3123.1));
        Point04Writable record2 = new Point04Writable(new IntWritable(1), new DoubleWritable(100.0));

        List<Point04Writable> input = Arrays.asList(record1, record2);

        Point04Writable assumedOutput = new Point04Writable(new IntWritable(3), new DoubleWritable(3223.1));

        new ReduceDriver<Text, Point04Writable, Text, Point04Writable>()
                .withReducer(new Point04Reducer())
                .withInput(new Text("154.62.81.*"), input)
                .withOutput(new Text("154.62.81.*"), assumedOutput)
                .runTest();
    }

}