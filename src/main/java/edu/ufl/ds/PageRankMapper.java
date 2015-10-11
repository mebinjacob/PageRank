package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		float p = 0;
		String[] split = value.toString().split(":");
		int noOfNeighbours = split[0].split(" ").length;
		float pageRank = 0.0f;
		if (!value.toString().contains(":")) { // this is the first
												// iteration
			pageRank = (float) 1 / (float) PageRanker.N;
		} else {
			pageRank = Float.parseFloat(split[split.length - 1].trim());
		}
		p = pageRank / (float) noOfNeighbours;

		// for all neighbours do the below..
		for (String neighbour : split[0].split(" ")) {
			if (!neighbour.trim().isEmpty()) {
				try {
					context.write(new Text(neighbour.trim()), new Text(
							new Float(p).toString()));
				} catch (Exception e) {
					System.out.println(e);
				}
			}
		}
		if (split[0].toString().trim().isEmpty())
			context.write(key, new Text("###"));
		else
			context.write(key, new Text(split[0].trim())); // pass
															// along
															// graph
															// structure
	}
}
