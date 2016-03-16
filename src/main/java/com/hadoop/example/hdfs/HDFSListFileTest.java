package com.hadoop.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSListFileTest {
	
	public static void main(String args[]) throws Exception{

		Configuration config = new Configuration();
		config.set("fs.default.name", "hdfs://server-a1:9000");
		
		FileSystem fs = FileSystem.get(config);
		FileStatus[] fileStatusList = fs.listStatus(new Path("/"));
		
		for(FileStatus fileStatus : fileStatusList){
			System.out.println(fileStatus.getPath().getName() + " " +
					           fileStatus.getLen() + " " +
					           fileStatus.getOwner() + ":" + 
					           fileStatus.getGroup());
		}
		fs.close();
		
	}
	
	
	

}
