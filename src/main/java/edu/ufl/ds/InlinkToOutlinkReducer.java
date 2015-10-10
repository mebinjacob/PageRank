package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InlinkToOutlinkReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		StringBuffer sb = new StringBuffer();
		if (!key.equals(" ")) {
			for (Text v : values) {
				if(!v.toString().trim().isEmpty())
					sb.append(v.toString() + "\t");
			}
			context.write(key, new Text(sb.toString()));
		} else {
			for (Text v : values) {
				if(!v.equals(" "))
				context.write(key, new Text(v));
			}
		}

	}

}
