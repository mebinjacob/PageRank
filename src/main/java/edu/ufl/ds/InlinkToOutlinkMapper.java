package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InlinkToOutlinkMapper extends
		Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
//		String[] titles = value.toString().split(" ");
/*		String val = titles[0];

		for (int i = 1; i < titles.length; i++) {
			context.write(new Text(titles[i]), new Text(val));
		}
		*/
//		String[] split = value.toString().split("	");
		System.out.println("The key is " + key);
		System.out.println("The value is " + value);
		context.write(new Text(value), new Text(key));
	}
}