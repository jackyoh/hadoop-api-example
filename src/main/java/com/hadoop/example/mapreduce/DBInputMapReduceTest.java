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
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DBInputMapReduceTest extends Configured implements Tool{
	
	public static class DBInputTestMapper
							extends Mapper<LongWritable, Employees, Text, IntWritable>{

		@Override
		public void map(LongWritable key, Employees value, Context context) 
                      					throws IOException, InterruptedException{
			//TODO
		}
	}

	public static class DBInputTestReducer
							extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                 							throws IOException, InterruptedException{
			//TODO
			
		}

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DBInputMapReduceTest(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("mapreduce.jdbc.driver.class", "com.mysql.jdbc.Driver");
		conf.set("mapreduce.jdbc.url", "jdbc:mysql://192.168.1.170:3306/dbtest");
		conf.set("mapreduce.jdbc.username", "root");
		conf.set("mapreduce.jdbc.password", "123456");
		
		Job job = Job.getInstance(conf, "database mapreduce example");
		job.setJarByClass(DBInputMapReduceTest.class);
		job.setMapperClass(DBInputTestMapper.class);
		job.setReducerClass(DBInputTestReducer.class);
		job.setInputFormatClass(DBInputFormat.class);
		
		String []fields = {"emp_no", "birth_date", "first_name", "last_name", "gender", "hire_date"};
		DBInputFormat.setInput(job, Employees.class, "employees", "", "emp_no", fields);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		return job.waitForCompletion(true) ? 0 : 1;	
	}

}


class Employees implements Writable, DBWritable {
    String emp_no;
	String birth_date;
	String first_name;
	String last_name;
	String gender;
	String hire_date;
	
	@Override
	public void write(PreparedStatement statement) throws SQLException {
		statement.setString(1, emp_no);
		statement.setString(2, birth_date);
		statement.setString(3, first_name);
		statement.setString(4, last_name);
		statement.setString(5, gender);
		statement.setString(6, hire_date);
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		this.emp_no = resultSet.getString(1);
		this.birth_date = resultSet.getString(2);
		this.first_name = resultSet.getString(3);
		this.last_name = resultSet.getString(4);
		this.gender = resultSet.getString(5);
		this.hire_date = resultSet.getString(6);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.emp_no);
		out.writeUTF(this.birth_date);
		out.writeUTF(this.first_name);
		out.writeUTF(this.last_name);
		out.writeUTF(this.gender);
		out.writeUTF(this.hire_date);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.emp_no = in.readUTF();
		this.birth_date = in.readUTF();
		this.first_name = in.readUTF();
		this.last_name = in.readLine();
		this.gender = in.readLine();
		this.hire_date = in.readLine();
	}	
	
}