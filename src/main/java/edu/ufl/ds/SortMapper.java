package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Text, Text, Text, Text> {
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		
		String[] split = value.toString().split(":");
		context.write(new Text(split[split.length-1]), key); 
	}
}
