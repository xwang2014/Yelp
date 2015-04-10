package yelp.reviewcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ViewCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable v : arg1) {
			sum += v.get();
		}
		
		if(sum > 0) {
			arg2.write(arg0, new IntWritable(sum));
		}
	}

}
