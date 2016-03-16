package com.hadoop.example.hdfs;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSReaderFile {
	
	public static void main(String args[]) throws Exception{
		
		Configuration config = new Configuration();
		config.set("fs.default.name", "hdfs://server-a1:9000");
		
		FileSystem fs = FileSystem.get(config);
	
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path("/outputhdfsfile1.txt"))));
		while(reader.ready()){
			System.out.println(reader.readLine());
		}
		fs.close();
		
		
	}

}
