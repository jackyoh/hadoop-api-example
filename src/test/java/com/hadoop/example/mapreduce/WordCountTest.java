package com.hadoop.example.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class WordCountTest {

	@Test
	public void testMapper() throws IOException{
		WordCount.TokenizerMapper tokenMapper = new WordCount.TokenizerMapper();
		MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = MapDriver.newMapDriver(tokenMapper);
		
		mapDriver.withInput(new LongWritable(), new Text("aaaa,bbbb"));
		mapDriver.withInput(new LongWritable(), new Text("cccc,dddd"));
		mapDriver.withOutput(new Text("aaaa"), new IntWritable(1));
		mapDriver.withOutput(new Text("bbbb"), new IntWritable(1));
		mapDriver.withOutput(new Text("cccc"), new IntWritable(1));
		mapDriver.withOutput(new Text("dddd"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReduce() throws IOException{
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		
		WordCount.IntSumReducer intSumReducer = new WordCount.IntSumReducer();
		ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver = ReduceDriver.newReduceDriver(intSumReducer);
		reduceDriver.withInput(new Text("aaaa"), values);
		reduceDriver.withOutput(new Text("aaaa"), new IntWritable(2));
		reduceDriver.runTest();		
	}
	
	@Test
	public void testMapReduce() throws IOException{
		WordCount.TokenizerMapper tokenMapper = new WordCount.TokenizerMapper();
		WordCount.IntSumReducer intSumReducer = new WordCount.IntSumReducer();
		
		MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver 
		                                            = MapReduceDriver.newMapReduceDriver(tokenMapper, intSumReducer);
		
		mapReduceDriver.withInput(new LongWritable(), new Text("aaaa,bbbb,cccc,aaaa,dddd"));
	    mapReduceDriver.withOutput(new Text("aaaa"), new IntWritable(2));
	    mapReduceDriver.withOutput(new Text("bbbb"), new IntWritable(1));
	    mapReduceDriver.withOutput(new Text("cccc"), new IntWritable(1));
	    mapReduceDriver.withOutput(new Text("dddd"), new IntWritable(1));
		mapReduceDriver.runTest();
	}

}
