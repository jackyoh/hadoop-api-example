package com.hadoop.example.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

public class FolderAndFileCount {
	
	public static class FolderAndFileMapper
			 extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable ONE = new IntWritable(1);
		
		@Override
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
			String firstStr = value.toString().substring(0, 1);
			context.write(new Text(firstStr), ONE);
		}
	}
	
	public static class FolderAndFileReduce
			 extends Reducer<Text,IntWritable,Text,IntWritable>{
		private Map<String, String> maps =  new HashMap<String, String>();
		
		@Override
		protected void setup(Context context){
			maps.put("-", "file");
			maps.put("d", "folder");
			maps.put("l", "link");
		}
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context
				           ) throws IOException, InterruptedException{
			int count = 0;
			for(IntWritable value : values){
				count = count + value.get();
			}
			context.write(new Text(maps.get(key.toString())), new IntWritable(count));
		}
	}
	
	public static void main(String args[]) throws Exception{
		 Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "folder and file count");
		 job.setJarByClass(FolderAndFileCount.class);
		 job.setMapperClass(FolderAndFileMapper.class);
		 job.setReducerClass(FolderAndFileReduce.class);
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 job.waitForCompletion(true);
	}
}
