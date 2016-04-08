package org.tkorostelev.homework04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Point02 extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Point02 <input_path> <output_path>");
            System.exit(-1);
        }

        final String inputPath = args[0];
        final String outputPath = args[1];

        Configuration conf = getConf();

        // Stage 1 is to aggregate and calculate, e.g. impression count

        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(Point02.class);
        job1.setJobName("Point02Stage1");

        FileInputFormat.addInputPath(job1, new Path(inputPath));

        Path stage1OutputPath = new Path(outputPath + "/stage1");
        FileOutputFormat.setOutputPath(job1, stage1OutputPath);
        SequenceFileOutputFormat.setCompressOutput(job1, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job1, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job1, SequenceFile.CompressionType.BLOCK);

        job1.setMapperClass(Point02Stage1Mapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setReducerClass(Point02Stage1Reducer.class);

        // Output key is impression count, value is iPinYouID
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);

        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        System.out.println("Running stage 1");
        boolean job1Result = job1.waitForCompletion(true);
        if(!job1Result) {
            return 1;
        }
        System.out.println("Stage 1 completed");

        // Stage 2 is to sort by impression count

        conf = getConf();
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(Point02.class);
        job2.setJobName("Point02Stage2");

        FileInputFormat.addInputPath(job2, new Path(outputPath + "/stage1/*"));
        job2.setInputFormatClass(SequenceFileInputFormat.class);

        FileOutputFormat.setOutputPath(job2, new Path(outputPath + "/stage2"));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setSortComparatorClass(Point02DescendingIntWritable.DescendingComparator.class);

        job2.setNumReduceTasks(3);

        job2.setPartitionerClass(Point02Partitioner.class);

        System.out.println("Running stage 2");
        return job2.waitForCompletion(true) ? 0 : 2;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Point02(), args);
        System.exit(exitCode);
    }
}
