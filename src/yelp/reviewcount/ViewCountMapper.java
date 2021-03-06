package yelp.reviewcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import yelp.data.business.Business;


public class ViewCountMapper extends Mapper<Text, Business, Text, IntWritable> {

	@Override
	protected void map(Text key, Business value,
			Context context)
			throws IOException, InterruptedException {
		context.write(new Text(value.name), new IntWritable((int)value.reviewCount));
	}

}
