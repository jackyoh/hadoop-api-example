package com.hadoop.example.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimpleMR2 {
	
	public static class TokenizerMapper
	   						extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new Text(""), new IntWritable(1));
		}
	}

	public static class IntSumReducer
    						extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, new IntWritable(1));
		}
	}

	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "simple MapReduce 2");
		job.setJarByClass(SimpleMR2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
	
}
