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
import java.io.IOException;

public class Q4 {

  public static class DifferenceMapper
    extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable negone = new IntWritable(-1);
    private Text node = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer result = new StringTokenizer(value.toString(), "\t");
      //use nextToken() to find the next value
      while(result.hasMoreTokens()) {
        node.set(result.nextToken());
        context.write(node, one);
        node.set(result.nextToken());
        context.write(node, negone);
      } 
    }
  }

  public static class DifferenceReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }


  public static class CountMapper
    extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text diff = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\t");
      while (tokenizer.hasMoreTokens()) {
        tokenizer.nextToken();
        diff.set(tokenizer.nextToken());
        context.write(diff, one);
      }
    }
  }

  public static class CountReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "job1");

    //First job
    job1.setJarByClass(Q4.class);
    job1.setMapperClass(DifferenceMapper.class);
    job1.setCombinerClass(DifferenceReducer.class);
    job1.setReducerClass(DifferenceReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1] + "first_output"));

    job1.waitForCompletion(true);

    //Second job
    Job job2 = Job.getInstance(conf, "job2");
    
    job2.setJarByClass(Q4.class);
    job2.setMapperClass(CountMapper.class);
    job2.setCombinerClass(CountReducer.class);
    job2.setReducerClass(CountReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job2, new Path(args[1] + "first_output"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
