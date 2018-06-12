package edu.umkc.cjk8zb;

/**
 * Created by Mayanka on 03-Sep-15.
 */


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

    private static int job1(Path inputPath, Path outputDir, String jobName) throws IOException,
            InterruptedException, ClassNotFoundException {

        // Create configuration
        Configuration conf = new Configuration(true);

        // Create job
        Job job = new Job(conf, jobName);
        job.setJarByClass(WordCountMapper.class);

        // Setup MapReduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setNumReduceTasks(1);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Delete output if exists
//        FileSystem hdfs = FileSystem.get(conf);
//        if (hdfs.exists(outputDir))
//            hdfs.delete(outputDir, true);

        // Execute job
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    private static int job2(Path inputPath, Path outputDir, String jobName) throws IOException,
            InterruptedException, ClassNotFoundException {

        // Create configuration
        Configuration conf = new Configuration(true);

        // Create job
        Job job = new Job(conf, jobName);
        job.setJarByClass(WordCount.class);

        // Setup MapReduce
        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer2.class);
        job.setNumReduceTasks(1);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Delete output if exists
//        FileSystem hdfs = FileSystem.get(conf);
//        if (hdfs.exists(outputDir))
//            hdfs.delete(outputDir, true);

        // Execute job
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        Path inputPath = new Path(args[0]);
        Path middlePath = new Path(args[1]);
        Path outputDir = new Path(args[2]);

        job1(inputPath, middlePath, "Job 1");
        job2(middlePath, outputDir, "Job 2");

        System.exit( 0 );

    }

}
