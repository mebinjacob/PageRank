package edu.ufl.ds;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class PageRanker extends Configured implements Tool {

	public static int N = 0; // total number of pages.

	public static void main(String[] args) throws Exception {
		String finalOutputDirectory = args[2];
		for (int i = 0; i < 8; i++) {
			args[2] = finalOutputDirectory + "_" + i; // output directory
			int res = ToolRunner.run(new Configuration(), new PageRanker(),
					args);
			// give output as input to next job
			args[0] = args[2];
		}
		// move to finalDirectory
//		 Files.move(new File(args[2]), new File(finalOutputDirectory));
	}

	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();
			conf.set(
					"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
					";");
			conf.set("mapreduce.output.textoutputformat.separator", ":");
			Job job = Job.getInstance(conf);
			job.setJarByClass(PageRanker.class);

			job.setMapperClass(PageRankMapper.class);

			job.setReducerClass(PageRankReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(KeyValueTextInputFormat.class);

			String content = Files.toString(new File(args[1]
					+ File.separatorChar + "part-r-00000"), Charsets.UTF_8);

			N = Integer.parseInt(content.split("=")[1].trim());

			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			job.setOutputFormatClass(TextOutputFormat.class);

			if (job.waitForCompletion(true))
				return 0;
			return -1;
		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}
}
