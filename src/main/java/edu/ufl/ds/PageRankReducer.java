package edu.ufl.ds;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, FloatWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		float sum = 0;
		Iterator<Text> iterator = values.iterator();
		String node = null;
		while (iterator.hasNext()) {
			Text val = iterator.next();
			try{
				Float s = Float.parseFloat(val.toString().trim());
				sum += s;
			}catch(NumberFormatException ne){
				node = val.toString();
			}
		}
		float d = 0.85f;
		sum = (1-d)/(float)PageRanker.N + sum*d;
		if(node != null && !node.trim().isEmpty()){
			if(node.equals("###")){
				context.write(new Text(key.toString()+ ";"), new FloatWritable(sum));	
			}
			else{
				context.write(new Text(key.toString()+ ";" + node), new FloatWritable(sum));
			}
			
		}
			
	}
}
