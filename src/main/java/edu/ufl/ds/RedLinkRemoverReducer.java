package edu.ufl.ds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RedLinkRemoverReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		boolean firstTime = false;
		List<String> outputList = new ArrayList<String>();
		Iterator<Text> iterator = values.iterator();
		while (iterator.hasNext()) {
			Text val = iterator.next();
			if (val.equals(key) && firstTime == false) {
				firstTime = true;
			} else {
				outputList.add(val.toString());
			}
		}
		if (firstTime == true) {
			if (outputList.size() == 0)
				context.write(new Text(" "), key);
			for (String output : outputList)
				context.write(key, new Text(output));
		}
	}

}
