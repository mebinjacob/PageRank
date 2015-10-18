package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRanker extends Configured implements Tool {

	public static int N = 0; // total number of pages.

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new PageRanker(), args);

	}

	public int run(String args[]) {
		try {
			int i = 0;
			String inputPath = args[0];
			String outputPath = args[1] + "" + i;
			while (i++ < 8) {
				Configuration conf = new Configuration();

				Job job = Job.getInstance(conf);
				job.setJarByClass(PageRanker.class);

				job.setMapperClass(PageRankMapper.class);
				job.setReducerClass(PageRankReducer.class);

				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				FileInputFormat.addInputPath(job, new Path(inputPath));
				job.setInputFormatClass(TextInputFormat.class);

				FileOutputFormat.setOutputPath(job, new Path(outputPath));
				job.setOutputFormatClass(TextOutputFormat.class);

				job.waitForCompletion(true);
				inputPath = outputPath;
				outputPath = args[1] + "" + i;
			}
			return 0;

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}

}
