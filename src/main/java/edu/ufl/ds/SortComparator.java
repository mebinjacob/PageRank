package edu.ufl.ds;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator{
	
	protected SortComparator(){
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2){
		Double firstValue = Double.parseDouble(((Text) w1).toString());
		Double secondValue = Double.parseDouble(((Text) w2).toString());
		return -1*firstValue.compareTo(secondValue);
	}
}