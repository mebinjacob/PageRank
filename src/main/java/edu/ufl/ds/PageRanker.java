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
<<<<<<< Updated upstream
		ToolRunner.run(new Configuration(), new PageRanker(), args);
=======
		String finalOutputDirectory = args[2];
		for (int i = 0; i < 8; i++) {
			args[2] = finalOutputDirectory + "_" + i; // output directory
			int res = ToolRunner.run(new Configuration(), new PageRanker(),
					args);
			// give output as input to next job
			args[0] = args[2];
		}
		// move to finalDirectory
		// Files.move(new File(args[2]), new File(finalOutputDirectory));
>>>>>>> Stashed changes
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

<<<<<<< Updated upstream
				job.setMapperClass(PageRankMapper.class);
				job.setReducerClass(PageRankReducer.class);

				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
=======
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(KeyValueTextInputFormat.class);
>>>>>>> Stashed changes

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
