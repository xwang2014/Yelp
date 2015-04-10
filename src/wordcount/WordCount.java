package wordcount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	public static void main(String[] args) throws Exception {
		long ts = System.currentTimeMillis();
		String input = "/data/xyz.txt";
		// String output = "hdfs://localhost:9000/result/output";
		String output = "/output/WordCount/" + "WordCount_" + ts;
		
		
//		JobConf conf = new JobConf(WordCount.class);
//		conf.setJobName("WordCount " + ts);
//
//		conf.setOutputKeyClass(Text.class);
//		conf.setOutputValueClass(IntWritable.class);
//
//		conf.setMapperClass(Map.class);
//		conf.setCombinerClass(Reduce.class);
//		conf.setReducerClass(Reduce.class);
//
//		conf.setInputFormat(TextInputFormat.class);
//		conf.setOutputFormat(TextOutputFormat.class);
//
//		if (args.length >= 2) {
//			input = args[0];
//			output = args[1];
//		}
//
//		FileInputFormat.setInputPaths(conf, new Path(input));
//		FileOutputFormat.setOutputPath(conf, new Path(output));
//
//		conf.setJarByClass(WordCount.class);
//		conf.setJarByClass(Map.class);
// 		conf.setJarByClass(Reduce.class);
//		
//		JobClient.runJob(conf);

		
		Configuration conf = new Configuration();
	        
		Job job = Job.getInstance(conf, "WordCount_" + System.currentTimeMillis());
	    
	    job.setMapperClass(WordCountMapper.class);
	    job.setReducerClass(WordCountReducer.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    TextInputFormat.addInputPath(job, new Path(input));
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    TextOutputFormat.setOutputPath(job, new Path(output));
	      
	    job.setJarByClass(WordCount.class);
	    
	    System.out.println(job.getJar());
	    job.waitForCompletion(true);
		
		
	}

}
