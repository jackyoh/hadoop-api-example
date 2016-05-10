package com.hadoop.example.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class FileSizeCount {

	public static class FileSizeMapper
	 				extends Mapper<LongWritable, Text, Text, IntWritable> {
	
		@Override
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
				String firstStr = value.toString().substring(0, 1);
				if(firstStr.equals("-")){
					String splits[] = value.toString().split(" ");
					List<String> strs = new ArrayList<String>();
					
					for(String split : splits){
						if(!split.equals("")){
						    strs.add(split);
						}
					}
					context.write(new Text(""), new IntWritable(Integer.parseInt(strs.get(4))));
				}
		}

	}
	
	public static class FileSizeReduce 
					extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context
				           ) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable value : values){
				sum = sum + value.get();
			}
			context.write(new Text(""), new IntWritable(sum));
		}
	}
	
	public static void main(String args[]) throws Exception{
		 Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "file size count");
		 job.setJarByClass(FileSizeCount.class);
		 job.setMapperClass(FileSizeMapper.class);
		 job.setReducerClass(FileSizeReduce.class);
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 job.waitForCompletion(true);
	}
}
