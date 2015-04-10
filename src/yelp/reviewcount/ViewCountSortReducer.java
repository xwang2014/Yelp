package yelp.reviewcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ViewCountSortReducer extends
		Reducer<IntWritable, Text, Text, IntWritable> {

	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1, Context arg2)
			throws IOException, InterruptedException {
		for (Text t : arg1) {
			arg2.write(t, arg0);
		}
	}

}