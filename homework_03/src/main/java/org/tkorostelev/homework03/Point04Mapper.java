package org.tkorostelev.homework03;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;

public class Point04Mapper
        extends Mapper<LongWritable, Text, Text, Point04Writable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        DatasetRow inputRow = DatasetRow.parseInputLine(value.toString());
        if (inputRow == null) {
            context.getCounter(Point04Counter.MALFORMED_ROW).increment(1);
            return;
        }

        context.getCounter(getBrowserType(inputRow.getUserAgent())).increment(1);

        context.write(new Text(inputRow.getIP()),
                new Point04Writable(
                        new IntWritable(1),
                        new DoubleWritable(inputRow.getBiddingPrice()))
        );
    }

    private static Point04BrowserCounter getBrowserType(String userAgent) {
        String userAgentLower = userAgent.toLowerCase();

        if(userAgentLower.contains("trident"))
            return Point04BrowserCounter.IE;
        else if(userAgentLower.contains("chrome"))
            return Point04BrowserCounter.CHROME;
        else if(userAgentLower.contains("opera"))
            return Point04BrowserCounter.OPERA;
        else if(userAgentLower.contains("mozilla"))
            return Point04BrowserCounter.MOZILLA;
        else
            return Point04BrowserCounter.OTHER;
    }

}
