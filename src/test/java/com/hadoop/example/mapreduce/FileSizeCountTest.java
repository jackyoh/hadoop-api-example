package com.hadoop.example.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class FileSizeCountTest {
	
	@Test
	public void testMapper() throws IOException{
		FileSizeCount.FileSizeMapper mapper = new FileSizeCount.FileSizeMapper();
		MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = MapDriver.newMapDriver(mapper);
		
		mapDriver.withInput(new LongWritable(), new Text("-rwxr-xr-x  1 root root        4121 2015-07-25 00:08 abrt1-to-abrt2"))
		         .withInput(new LongWritable(), new Text("drwxr-xr-x. 2 root root        4096 2015-08-15 03:46 awk"))
		         .withInput(new LongWritable(), new Text("-rwxr-xr-x  1 root root        5583 2015-07-25 00:07 abrt-action-ureport"))
		         .withOutput(new Text(""), new IntWritable(4121))
		         .withOutput(new Text(""), new IntWritable(5583));
		mapDriver.runTest();
	}
	
	@Test
	public void testReduce() throws IOException{
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(4121));
		values.add(new IntWritable(5583));
		
		FileSizeCount.FileSizeReduce reducer = new FileSizeCount.FileSizeReduce();
		ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver = ReduceDriver.newReduceDriver(reducer);
		
		reduceDriver.withInput(new Text(""), values)
					.withOutput(new Text(""), new IntWritable(9704));

		reduceDriver.runTest();
	}

}
