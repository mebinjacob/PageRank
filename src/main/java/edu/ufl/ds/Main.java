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

		String[] args_1 = { args[0], args[1] + "/intermediate" };
		RedLinkRemover.main(args_1);
		String[] args_2 = { args[1] + "/intermediate", args[1] + "/outlinks" };
		InlinkToOutlinkConverter.main(args_2);
		String[] args_3 = { args[1] + "/outlinks", args[1] + "/nodeCount" };
		PageCount.main(args_3);

		String[] args_4 = { args[1] + "/outlinks", args[1] + "/cleaned" };// "src/main/resources/output/results"
		Cleaner.main(args_4);
		String[] args_5 = { args[1] + "/cleaned",
				args[1] + "/iterations" };// "src/main/resources/output/results"
		PageRanker.main(args_5);
		String[] args_6 = { args[1] + "/iterations7",
				args[1] + "/sort_result" };
		PageSort.main(args_6);
		
		String[] args_7 = { args[1] + "/iterations6",
				args[1] + "/sort_result6" };
		PageSort.main(args_7);
		
		String[] args_8 = { args[1] + "/iterations1",
				args[1] + "/sort_result1" };
		PageSort.main(args_8);
		Path dist = new Path(args[1]);
		Configuration conf = new Configuration();
		 FileSystem fs = FileSystem.get(conf);
		// if (!fs.exists(dist))
		// fs.mkdirs(dist);
		// fs.rename(new Path("src/main/resources/output/iterations7"
		// + "/part-r-00000"), new Path(args[1] + "/PageRank.iter8.out"));
		// fs.rename(new Path("src/main/resources/output/iterations0" + "/"
		// + "part-r-00000"), new Path(args[1] + "/PageRank.iter1.out"));
		// fs.rename(new Path("src/main/resources/output/sort_result"
		// + "/part-r-00000"), new Path(args[1] + "/PageRank.sorted.out"));
		// fs.rename(new Path("src/main/resources/output/cleaned"
		// + "/part-r-00000"), new Path(args[1] + "/PageRank.outlink.out"));
		// fs.rename(new Path("src/main/resources/output/nodeCount"
		// + "/part-r-00000"), new Path(args[1] + "/PageRank.n.out"));

		return 0;
	}
}
