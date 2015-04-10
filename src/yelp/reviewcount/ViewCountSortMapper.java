package yelp.reviewcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import yelp.YelpDriver.BUSINESS_NAME_NATURE;

public class ViewCountSortMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		StringTokenizer tokenizer = new StringTokenizer(line);
		String name = "EMPTY";
		String count = "";

		while (tokenizer.hasMoreTokens()) {
			count = tokenizer.nextToken();
		}

		name = line.substring(0, line.length() - count.length());
		int cnt = Integer.parseInt(count);

		if(name.startsWith("[0-9]+")) {
			context.getCounter(BUSINESS_NAME_NATURE.START_WITH_DIGIT).increment(1);
		} else {
			context.getCounter(BUSINESS_NAME_NATURE.START_WITH_LETTER).increment(1);
		}
		
		context.write(new IntWritable(cnt), new Text(name));
	}

}