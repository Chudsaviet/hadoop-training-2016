package org.tkorostelev.homework04;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Point02Partitioner extends Partitioner<IntWritable, Text> {
    @Override
    public int getPartition(IntWritable key, Text value, int numPartitions) {
        // Basic partitioner. numPartitions > 3 is threated as 3
        // Ones goes to first partition
        // <1000 goes to second
        // Others goes to third

        if (numPartitions == 1)
        {
            return 0;
        }
        else if (numPartitions == 2) {
            if (key.get() <= 1)
                return 0;
            else
                return 1;
        }
        else {
            if (key.get() <= 1)
                return 0;
            else if (key.get() < 1000)
                return 1;
            else
                return 2;
        }

    }
}
