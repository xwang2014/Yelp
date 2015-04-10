package wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	
	
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Context arg2)
			throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable v : arg1) {
			sum += v.get();
		}
		
		arg2.write(arg0, new IntWritable(sum));
	}

}