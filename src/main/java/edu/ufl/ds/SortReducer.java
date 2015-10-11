package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if (Float.parseFloat(key.toString().trim()) > 5 / (float) PageRanker.N)
			for (Text t : values) {
				context.write(new Text(t), key);
			}
	}
}
