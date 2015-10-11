package edu.ufl.ds;

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

public class PageSort extends Configured implements Tool {

	public static int N = 0; // total number of pages.

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new PageSort(),args);
	}

	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();
			conf.set(
					"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
					";");
			conf.set("mapreduce.output.textoutputformat.separator", "\t");
			Job job = Job.getInstance(conf);
			job.setSortComparatorClass(SortComparator.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setJarByClass(PageSort.class);

			job.setMapperClass(SortMapper.class);

			job.setReducerClass(SortReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(KeyValueTextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1]));

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
