package com.hadoop.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSMkdirTest {

	public static void main(String args[]) throws Exception{
	
		Configuration config = new Configuration();
		config.set("fs.default.name", "hdfs://server-a1:9000");
		
		FileSystem fs = FileSystem.get(config);
	    fs.mkdirs(new Path("/folder1"));
	
	    boolean isExists = fs.exists(new Path("/folder1"));
		System.out.println(isExists);
	    
		fs.close();		
	}
	
}
