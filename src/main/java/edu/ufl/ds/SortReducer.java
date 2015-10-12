package edu.ufl.ds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		TreeMap<Double, List<String>> map = new TreeMap<>(
				new Comparator<Double>() {
					@Override
					public int compare(Double o1, Double o2) {
						return o2.compareTo(o1);
					}
				});
		for (Text t : values) {
			String[] v = t.toString().split("\t");
			double rank = Double.parseDouble(v[1]);
			if (rank >= 5 / PageCount.n) {
				List<String> list = map.get(rank);
				if (list == null) {
					list = new ArrayList<>();
					map.put(rank, list);
				}
				list.add(v[0]);
			}
		}
		for (Double d : map.keySet()) {
			List<String> list = map.get(d);
			for (String s : list) {
				context.write(new Text(s), new Text(d + ""));
			}
		}
	}
}
