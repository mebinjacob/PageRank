package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InlinkToOutlinkMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] titles = value.toString().split(" ");
		String val = titles[0];
		if (titles.length == 1) {
			context.write(new Text(val.trim()), new Text(""));
		}
		if (val.trim().equals("===")) {
			val = "";
		} else
			context.write(new Text(val.trim()), new Text(""));
		for (int i = 1; i < titles.length; i++) {
			context.write(new Text(titles[i]), new Text(val));
		}
	}
}