package org.tkorostelev.homework04;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Point02Stage1Reducer
        extends Reducer<Text, IntWritable, IntWritable, Text> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        int impressionCount=0;
        for (IntWritable value: values) {
            impressionCount += value.get();
        }

        // We swap key and value for output
        context.write(new IntWritable(impressionCount), key);
    }
}
