package org.tkorostelev.homework04;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Point01Mapper
        extends Mapper<LongWritable, Text, Point01Writable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        DatasetRow inputRow = DatasetRow.parseInputLine(value.toString());
        if (inputRow == null) {
            context.getCounter(Point01ErrorCounter.MALFORMED_ROW).increment(1);
            return;
        }

        context.write(
                new Point01Writable(new Text(inputRow.getIPinYouID()), new Text(inputRow.getTimestamp())),
                value
        );
    }

}
