package com.hadoop.example.hdfs;

import java.io.DataOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSWriteFile {

	public static void main(String args[]) throws Exception{
		
		Configuration config = new Configuration();
		config.set("fs.default.name", "hdfs://server-a1:9000");
		
		FileSystem fs = FileSystem.get(config);
		OutputStream outputStream = fs.create(new Path("/outputhdfsfile1.txt"), false);
		DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
		dataOutputStream.writeUTF("write line 1\n");
		dataOutputStream.writeUTF("write line 2\n");
		dataOutputStream.writeUTF("write line 3\n");
		
		dataOutputStream.close();
		outputStream.close();
		fs.close();
		
	}
	
}
