package org.tkorostelev.homework03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Point04 extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Point04 <input_path> <output_path>");
            System.exit(-1);
        }

        final String inputPath = args[0];
        final String outputPath = args[1];

        final Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setJarByClass(Point04.class);
        job.setJobName("Point04");

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(Point04Mapper.class);
        job.setCombinerClass(Point04Reducer.class);
        job.setReducerClass(Point04Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Point04Writable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Point04(), args);
        System.exit(exitCode);
    }
}
