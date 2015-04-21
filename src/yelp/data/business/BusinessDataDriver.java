package yelp.data.business;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import yelp.YelpCommon;

/**
 * This class handles the yelp business source data
 * @author xin
 *
 */
public class BusinessDataDriver {
	
	Configuration conf = null;
	
	
	
	public static void main(String[] args) {
		BusinessDataDriver driver = new BusinessDataDriver();
		
		try {
			driver.execute();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public void execute() throws IOException {
		this.conf = new Configuration();
		
		Job job = new Job(conf); 
		job.setJobName("Yelp Business Data Init");
		//job.setJarByClass(getClass());
		job.setJarByClass(BusinessDataDriver.class);
		
		job.setMapperClass(BusinessDataMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Business.class);
		
		job.setReducerClass(BusinessDataReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Business.class);
		
		String input = "/yelp/raw/business/";
		String output = "/data/yelp/business/";
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(input));

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setNumReduceTasks(YelpCommon.partitionNum);
		
		boolean flag = false;
		try {
			flag = job.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		System.out.println("Job result : " + flag);
	}
	
	static class BusinessDataMapper extends Mapper<LongWritable, Text, Text, Business> {

		
		
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {

			JSONParser parser = new JSONParser();
			try {
				JSONObject obj = (JSONObject) parser.parse(value.toString());
				Business business = Business.readFromJson(obj);
				
				Text shuffleKey = new Text(business.businessId);
				
				context.write(shuffleKey, business);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
		}


		
	}

	static class BusinessDataReducer extends Reducer<Text, Business, NullWritable, Business> {


		@Override
		protected void reduce(Text arg0, Iterable<Business> arg1,
				Context arg2)
				throws IOException, InterruptedException {
			
			for(Business bus : arg1) {
				//mos.write(NullWritable.get(), bus, "/data/yelp/business/");
				arg2.write(NullWritable.get(), bus);
			}
			
		}

	}
}
