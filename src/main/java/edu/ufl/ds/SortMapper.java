package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			String[] split = value.toString().split("\t");
			context.write(new Text("----"),
					new Text(split[0] + "\t" + split[1]));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
