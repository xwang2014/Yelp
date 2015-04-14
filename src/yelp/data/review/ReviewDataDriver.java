package yelp.data.review;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ReviewDataDriver {
	
	private static final int partitionNum = 10;
	
	public static class ReviewDataMapper extends Mapper<LongWritable, Text, LongWritable, Review> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//super.map(key, value, context);
			JSONParser parser = new JSONParser();
			JSONObject obj = null;
			try {
				obj = (JSONObject) parser.parse(value.toString());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			Review review = Review.readFromJson(obj);
			long shuffleKey = review.businessId.hashCode() % partitionNum;
			
			context.write(new LongWritable(shuffleKey), review);
		}
		
	}
	
	public static class ReviewDataReducer extends Reducer<LongWritable, Review, NullWritable, Review> {

		@Override
		protected void reduce(LongWritable arg0, Iterable<Review> arg1,
				Context arg2)
				throws IOException, InterruptedException {
			for(Review r : arg1) {
				arg2.write(NullWritable.get(), r);
			}
		}
		
	}
	
	public void work() throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf);
		job.setJobName("Review Data Init");
		job.setJarByClass(ReviewDataDriver.class);
		
		job.setMapperClass(ReviewDataMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Review.class);
		
		job.setReducerClass(ReviewDataReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Review.class);
		
		Path input = new Path("/yelp/raw/review");
		Path output = new Path("/data/yelp/reivew");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output)) {
			fs.delete(output, true);
		}
		//fs.mkdirs(output);
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, input);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);
		
		job.setNumReduceTasks(partitionNum);
		
		boolean flag = false;
		flag = job.waitForCompletion(true);
		
		System.out.println("Job result = " + flag);
	}

	public static void main(String[] args) {
		ReviewDataDriver driver = new ReviewDataDriver();
		
		try {
			driver.work();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}
