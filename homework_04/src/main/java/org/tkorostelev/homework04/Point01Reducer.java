package org.tkorostelev.homework04;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Point01Reducer
        extends Reducer<Point01Writable, Text, NullWritable, Text> {

    @Override
    public void reduce(Point01Writable key, Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(NullWritable.get(), value);
        }
    }
}
