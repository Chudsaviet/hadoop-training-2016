package org.tkorostelev.homework03;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class Point03 {

    public static void run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Point03 <tags_file> <input_path> <output_path>");
            System.exit(-1);
        }

        final String tagsFile = args[0];
        final String inputPath = args[1];
        final String outputPath = args[2];

        final URI tagsFileURI = new URI(tagsFile);

        Job job = Job.getInstance();
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

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        run(args);
    }
}
