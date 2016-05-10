package com.hadoop.example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DBInputMapReduce {
	
	public static class DBInputMapper
	 				extends Mapper<LongWritable, Text, Text, IntWritable>{
	

	}
	
	public static class DBInputReducer
					extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		
		
	}
	
	public static void main(String args[]) throws Exception{
		
		 Configuration conf = new Configuration();
		
		 Job job = Job.getInstance(conf, "database mapreduce example");
		
		 DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.1.176", "root", "123456");

		 
		 job.setJarByClass(DBInputMapReduce.class);
		 job.setMapperClass(DBInputMapper.class);
		 job.setReducerClass(DBInputReducer.class);
		 job.setInputFormatClass(DBInputFormat.class);
		// DBInputFormat.setInput(job, DBWritable.class, inputQuery, );
		 
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 job.waitForCompletion(true);		
	}

}
