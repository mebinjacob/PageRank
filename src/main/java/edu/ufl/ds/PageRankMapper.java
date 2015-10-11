package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		float p = 0;
		int noOfNeighbours = value.toString().split(":")[0].split(" ").length;
		FloatWritable pageRank = null;
		if (value.toString().split(":").length != 2) { // this is the first
														// iteration
			pageRank = new FloatWritable((float) 1 / PageRanker.N);
		} else {
			pageRank = new FloatWritable(Float.parseFloat(value.toString()
					.split(":")[1].trim()));
		}
		p = pageRank.get() / noOfNeighbours;
		// -- should not be required

		// for all neighbours do the below..
		for (String neighbour : value.toString().split(":")[0].split("\t")) {
			if (!neighbour.trim().isEmpty()) {
				try {
					context.write(new Text(neighbour),
							new Text(new Float(p).toString()));
				} catch (Exception e) {
					System.out.println(e);
				}
			}
		}
		context.write(new Text(key), new Text(value.toString().trim())); // pass
																			// along
																			// graph
																			// structure
	}
}
