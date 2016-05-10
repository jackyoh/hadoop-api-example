package com.hadoop.example.mapreduce;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class FileTextSplit {
	
	@Test
	public void testSplit(){
		String str = "-rwxr-xr-x  1 root root        4121 2015-07-25 00:08 abrt1-to-abrt2";
		String splits[] = str.split(" ");
		List<String> strs = new ArrayList<String>();
		
		for(String split : splits){
			if(!split.equals("")){
			    strs.add(split);
			}
		}
		assertEquals("4121", strs.get(4));
	}

}
