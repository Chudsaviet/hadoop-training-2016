package org.tkorostelev.homework03;

import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Homework03 {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: Homework03 <tags file>");
      System.exit(-1);
    }

    final String tagsFile = args[0];
        
    Job job = new Job();
    job.setJarByClass(Homework03.class);
    job.setJobName("Homework03");

    job.addCacheArchive(new URI(tagsFile));
//    FileInputFormat.addInputPath(job, new Path(args[0]));
//    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
//    job.setMapperClass(MaxTemperatureMapper.class);
//    job.setReducerClass(MaxTemperatureReducer.class);

//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(IntWritable.class);
    
//    System.exit(job.waitForCompletion(true) ? 0 : 1);

    System.exit(0);
  }
}
