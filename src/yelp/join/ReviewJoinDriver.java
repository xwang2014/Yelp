package yelp.join;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import yelp.YelpCommon;
import yelp.data.business.Business;
import yelp.data.review.Review;

/*
 * This class test for reduce-side join
 */
public class ReviewJoinDriver {
	
	public enum CNT {
		TOTAL,
		MATCHED,
		PERFECT
	}
	
	
	public void execute() throws IOException, InterruptedException, ClassNotFoundException {
		Path reviewInputPath = new Path("/data/yelp/reivew/");
		Path businessInputPath = new Path("/data/yelp/business/");
		Path outputPath = new Path("/data/yelp/join/");
		
		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(getClass());
		job.setJobName("Yelp Multi Datasets Join Test");
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		job.setReducerClass(ReviewJoinReducer.class);
		job.setNumReduceTasks(YelpCommon.partitionNum);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ReviewJoinValue.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, reviewInputPath, 
				SequenceFileInputFormat.class, ReviewJoinDataMapper.class);
		MultipleInputs.addInputPath(job, businessInputPath, 
				SequenceFileInputFormat.class, ReviewJoinBusinessMapper.class);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
	}
	
	public static class ReviewJoinDataMapper extends Mapper<NullWritable, Review, Text, ReviewJoinValue> {

		@Override
		protected void map(NullWritable key, Review value, Context context)
				throws IOException, InterruptedException {
			// 
			Text okey = new Text(value.businessId);
			ReviewJoinValue ovalue = new ReviewJoinValue();
			ovalue.businessId = value.businessId;
			ovalue.isBusData = false;
			ovalue.busData = null;
			
			context.write(okey, ovalue);
		}
		
	}
	
	public static class ReviewJoinBusinessMapper extends Mapper<NullWritable, Business, Text, ReviewJoinValue> {

		@Override
		protected void map(NullWritable key, Business value, Context context)
				throws IOException, InterruptedException {
			// 
			Text okey = new Text(value.businessId);
			ReviewJoinValue ovalue = new ReviewJoinValue();
			ovalue.businessId = value.businessId;
			ovalue.isBusData = true;
			ovalue.busData = value;
			
			context.write(okey, ovalue);
		}
		
	}
	
	
	public static class ReviewJoinReducer extends Reducer <Text, ReviewJoinValue, Text, Text> {

		@Override
		protected void reduce(Text arg0, Iterable<ReviewJoinValue> arg1,
				Context arg2)
				throws IOException, InterruptedException {
			// 
			ReviewJoinValue busData = null;
			int reviewCnt = 0;
			for(ReviewJoinValue v : arg1) {
				if(v.isBusData) {
					busData = v;
				} else {
					reviewCnt++;
				}
			}
			
			
			if(busData != null && busData.busData != null) {
				Text oKey = new Text(busData.busData.name + " --- " + busData.busData.reviewCount);
				Text oValue = new Text("" + reviewCnt);
				arg2.write(oKey, oValue);
				
				arg2.getCounter(CNT.MATCHED).increment(1);
				
				if(busData.busData.reviewCount == reviewCnt) {
					arg2.getCounter(CNT.PERFECT).increment(1);
				}
			} else {
				arg2.write(new Text(arg0.toString() + " --- -1"), new Text("" + reviewCnt));
			}
			
			arg2.getCounter(CNT.TOTAL).increment(1);
		}
		
	}

	public static void main(String[] args) {
		ReviewJoinDriver driver = new ReviewJoinDriver();
		
		try {
			driver.execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
