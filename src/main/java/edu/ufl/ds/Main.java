package edu.ufl.ds;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Main(), args);
	}

	@Override
	public int run(String[] args) throws Exception {

		Path dist = new Path(args[1]);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if (!fs.exists(dist))
			fs.mkdirs(dist);
		String[] args_1 = { args[0], "src/main/resources/output/intermediate" };
		RedLinkRemover.main(args_1);
		String[] args_2 = { "src/main/resources/output/intermediate",
				"src/main/resources/output/outlinks" };
		InlinkToOutlinkConverter.main(args_2);
		String[] args_3 = { "src/main/resources/output/outlinks",
				"src/main/resources/output/nodeCount" };
		PageCount.main(args_3);
		String[] args_4 = { "src/main/resources/output/outlinks",
				"src/main/resources/output/nodeCount",
				"src/main/resources/output/iterations" };// "src/main/resources/output/results"
		PageRanker.main(args_4);
		String[] args_5 = { "src/main/resources/output/iterations_7",
				"src/main/resources/output/sort_result" };
		PageSort.main(args_5);

		fs.rename(new Path("src/main/resources/output/iterations_7"
				+ "/part-r-00000"), new Path(args[1] + "/PageRank.iter8.out"));
		fs.rename(new Path("src/main/resources/output/iterations_0" + "/"
				+ "part-r-00000"), new Path(args[1] + "/PageRank.iter1.out"));
		fs.rename(new Path("src/main/resources/output/sort_result"
				+ "/part-r-00000"), new Path(args[1] + "/PageRank.sorted.out"));
		fs.rename(new Path("src/main/resources/output/outlinks"
				+ "/part-r-00000"), new Path(args[1] + "/PageRank.outlink.out"));
		fs.rename(new Path("src/main/resources/output/nodeCount"
				+ "/part-r-00000"), new Path(args[1] + "/PageRank.n.out"));

		return 0;
	}
}
