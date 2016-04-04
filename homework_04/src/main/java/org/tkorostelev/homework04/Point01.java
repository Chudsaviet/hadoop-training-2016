package org.tkorostelev.homework04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Point01 extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Point01 <input_path> <output_path>");
            System.exit(-1);
        }

        final String inputPath = args[0];
        final String outputPath = args[1];

        final Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setJarByClass(Point01.class);
        job.setJobName("Point01");

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(Point01Mapper.class);
        job.setReducerClass(Point01Reducer.class);

        job.setOutputKeyClass(Point01Writable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Point01(), args);
        System.exit(exitCode);
    }
}
