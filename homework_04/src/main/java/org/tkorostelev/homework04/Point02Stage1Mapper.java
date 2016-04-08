package org.tkorostelev.homework04;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Point02Stage1Mapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        DatasetRow inputRow = DatasetRow.parseInputLine(value.toString());
        if (inputRow == null) {
            context.getCounter(Point02ErrorCounter.MALFORMED_ROW).increment(1);
            return;
        }

        if (inputRow.getStreamID() == 1) {
            context.write(
                    new Text(inputRow.getIPinYouID()),
                    new IntWritable(1)
            );
        }
    }

}
