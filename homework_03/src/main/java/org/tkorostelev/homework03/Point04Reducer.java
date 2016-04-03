package org.tkorostelev.homework03;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Point04Reducer
        extends Reducer<Text, Point04Writable, Text, Point04Writable> {

    @Override
    public void reduce(Text key, Iterable<Point04Writable> values,
                       Context context)
            throws IOException, InterruptedException {

        int ipOccurences = 0;
        double biddingSum = 0;
        for (Point04Writable value : values) {
            ipOccurences += value.getIpOccurences().get();
            biddingSum += value.getBiddingSum().get();
        }

        context.write(key,
                new Point04Writable(
                        new IntWritable(ipOccurences),
                        new DoubleWritable(biddingSum))
        );
    }
}
