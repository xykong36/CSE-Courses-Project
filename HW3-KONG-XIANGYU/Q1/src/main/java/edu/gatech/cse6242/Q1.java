package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Object;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {

  public static class TargetWeightMapper
       extends Mapper<Object, Text, IntWritable, IntWritable>{

    private IntWritable email = new IntWritable();
    private IntWritable weight = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer token = new StringTokenizer(value.toString(), "\n");
      while (token.hasMoreTokens()) {
        String word = token.nextToken();
        String tokens[] = word.split("\t");
        //set the value of the email and weight variable
        email.set(Integer.parseInt(tokens[1]));
        weight.set(Integer.parseInt(tokens[2]));
        context.write(email, weight);
      }
    }
  }

  public static class WeightSumReducer
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
     
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int min = 1000000;
      for (IntWritable val : values) {
        if(val.get() < min) {
          min = val.get();
        }
      }
      result.set(min);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");

    job.setJarByClass(Q1.class);
    job.setMapperClass(TargetWeightMapper.class);
    job.setCombinerClass(WeightSumReducer.class);
    job.setReducerClass(WeightSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
