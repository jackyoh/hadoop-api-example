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

public class SimpleMR {
	public static class SimpleMapper
	     				   extends Mapper<LongWritable, Text, LongWritable, Text>{
		  @Override
		  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			   context.write(key, value);
		  }
	}
	
	public static class SimpleReducer
	                       extends Reducer<LongWritable, Text, LongWritable, Text> {
		  @Override
		  public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  for(Text value : values){
				  context.write(key, value);
			  }
		  }
	}
	
	public static void main(String args[]) throws Exception{
		 Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "simple MapReduce");
		 job.setJarByClass(SimpleMR.class);
		 job.setMapperClass(SimpleMapper.class);
		 job.setReducerClass(SimpleReducer.class);
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 job.waitForCompletion(true);
	}
}