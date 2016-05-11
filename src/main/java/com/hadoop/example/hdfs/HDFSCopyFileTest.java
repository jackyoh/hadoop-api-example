package com.hadoop.example.hdfs;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HDFSCopyFileTest {

	public static void main(String args[]) throws Exception{
		Configuration config = new Configuration();
		config.set("fs.default.name", "hdfs://yddev-node1:8020");
		
		FileSystem fs = FileSystem.get(config);
		
		String path = HDFSCopyFileTest.class.getResource("/").getPath();
		File srcPath = new File(new File(path).getParent(), "/data/libexec.dat");
		
		if(srcPath.exists()){
			FileUtil.copy(srcPath, fs, new Path("/tmp"), false, config);
			System.out.println("Upload to HDFS finish");
		}else{
			System.out.println("Source File Not Exists");
		}
		fs.close();
	}
	
}
