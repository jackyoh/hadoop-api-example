package com.hadoop.example.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DBInputMapReduce extends Configured implements Tool{
	
	public static class DBInputMapper
	 				extends Mapper<LongWritable, Table1, Text, IntWritable>{
	
		@Override
		public void map(LongWritable key, Table1 value, Context context) 
				                      throws IOException, InterruptedException{
			context.write(new Text(value.column1), new IntWritable(1));
		}
	}
	
	public static class DBInputReducer
					extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				                 throws IOException, InterruptedException{
			int count = 0;
			for(IntWritable value : values){
				count = count + value.get();
			}
			context.write(key, new IntWritable(count));
		}
		
	}

	public static void main(String[] args) throws Exception {
		  ToolRunner.run(new DBInputMapReduce(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		 Configuration conf = getConf();
		 conf.set("mapreduce.jdbc.driver.class", "com.mysql.jdbc.Driver");
		 conf.set("mapreduce.jdbc.url", "jdbc:mysql://192.168.1.176/dbtest");
		 conf.set("mapreduce.jdbc.username", "root");
		 conf.set("mapreduce.jdbc.password", "123456");
		 
		 Job job = Job.getInstance(conf, "database mapreduce example");
		 job.setJarByClass(DBInputMapReduce.class);
		 job.setMapperClass(DBInputMapper.class);
		 job.setReducerClass(DBInputReducer.class);
		 job.setInputFormatClass(DBInputFormat.class);
		 
		 String []fields = {"column1", "column2"};
		 DBInputFormat.setInput(job, Table1.class, "table1", "", "column1", fields);
		 
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 FileOutputFormat.setOutputPath(job, new Path(args[0]));
		 return job.waitForCompletion(true) ? 0 : 1;	
	}
}

class Table1 implements Writable, DBWritable {
    String column1;
	String column2;
	
	@Override
	public void write(PreparedStatement statement) throws SQLException {
		statement.setString(1, this.column1);
		statement.setString(2, this.column2);
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		this.column1 = resultSet.getString(1);
		this.column2 = resultSet.getString(2);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.column1);
		out.writeUTF(this.column2);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.column1 = in.readUTF();
		this.column2 = in.readUTF();
	}	
}