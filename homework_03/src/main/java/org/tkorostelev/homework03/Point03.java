package org.tkorostelev.homework03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class Point03 extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Point03 <tags_file> <input_path> <output_path>");
            System.exit(-1);
        }

        final String tagsFile = args[0];
        final String inputPath = args[1];
        final String outputPath = args[2];

        final URI tagsFileURI = new URI(tagsFile);

        final Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setJarByClass(Point03.class);
        job.setJobName("Point03");

        job.addCacheFile(new URI(tagsFile));
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(Point03Mapper.class);
        job.setCombinerClass(Point03Reducer.class);
        job.setReducerClass(Point03Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Point03(), args);
        System.exit(exitCode);
    }
}
