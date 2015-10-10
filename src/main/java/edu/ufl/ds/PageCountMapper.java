package edu.ufl.ds;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
    private final IntWritable one = new IntWritable(1);
    private Text word = new Text("N");

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	context.write(word, one);
    }
} 