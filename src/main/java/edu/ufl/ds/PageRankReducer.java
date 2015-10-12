package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
<<<<<<< Updated upstream

		long N = 19999;
		double d = 0.85;
		double rank = (1 - d) / N;
		double remaining = 0;

		StringBuffer sb = new StringBuffer();
		String prefix = "";
		for (Text t : values) {
			try {
				remaining += Double.parseDouble(t.toString());
			} catch (Exception e) {
				sb.append(prefix + t.toString());
				prefix = "\t";
			}
		}

		rank += d * remaining;

		context.write(key, new Text(rank + "\t" + sb.toString()));
=======
		float sum = 0;
		Iterator<Text> iterator = values.iterator();
		String node = null;
		while (iterator.hasNext()) {
			Text val = iterator.next();
			try {
				Float s = Float.parseFloat(val.toString().trim());
				sum += s;
			} catch (NumberFormatException ne) {
				node = val.toString();
			}
		}
		float d = 0.85f;
		sum = (1 - d) / (float) PageRanker.N + sum * d;
		if (node != null && !node.trim().isEmpty()) {
			if (node.equals("###")) {
				context.write(new Text(key.toString() + ";"),
						new Text(sum + ""));
			} else {
				context.write(new Text(key.toString() + ";" + node), new Text(
						sum + ""));
			}

		}

>>>>>>> Stashed changes
	}
}
