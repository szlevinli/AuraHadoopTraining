package org.training.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class WordCountSort {

  public static class TokenizerMapper
      extends Mapper<Text, Text, IntWritable, Text> {

    private IntWritable intSum = new IntWritable();

    @Override
    public void map(Text word, Text count, Context context
    )
        throws IOException, InterruptedException {
      int intCount = Integer.parseInt(count.toString());
      intSum.set(intCount);
      context.write(intSum, word);
    }
  }

  public static void main(String[] args)
      throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }

    // create job
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCountSort.class);

    // Mapper configuration
    job.setMapperClass(TokenizerMapper.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);

    // Reducer configuration
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
        new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

