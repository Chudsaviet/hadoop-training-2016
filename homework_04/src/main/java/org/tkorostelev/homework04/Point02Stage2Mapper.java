package org.tkorostelev.homework04;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Point02Stage2Mapper
        extends Mapper<IntWritable, Text, IntWritable, Text> {

    @Override
    public void map(IntWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        context.write(key, value);
    }

}
