package yelp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import yelp.reviewcount.ViewCountMapper;
import yelp.reviewcount.ViewCountReducer;
import yelp.reviewcount.ViewCountSortMapper;
import yelp.reviewcount.ViewCountSortReducer;


public class YelpDriver {
	
	public enum BUSINESS_NAME_NATURE {
		START_WITH_DIGIT,
		START_WITH_LETTER
	}
	
	/*
	 * org.apache.commons.logging.Log;
	 */
	//protected static final Log log = LogFactory.getLog(YelpDriver.class);

	/*
	 * org.apache.log4j.Logger
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	//protected static final Logger log = Logger.getLogger(YelpDriver.class);

	
	public boolean maxViewCount(String[] args) throws IOException {
		long ts = System.currentTimeMillis();
		String input = "/data/yelp/";
		String outpath = "/output/yelp/maxViewCount_" + ts;
		if(args != null && args.length > 0) {
			input = args[0];
		}
		if(args != null && args.length > 1) {
			outpath = args[1];
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName( "YelpReviewCount_" + ts);
		
		job.setJarByClass(ViewCountMapper.class);
		
		job.setMapperClass(ViewCountMapper.class);
		job.setReducerClass(ViewCountReducer.class);
		
		// When read a text file with a M/R program, the default input key of the mapper 
		// would be the index of the line in the file, while the default input value 
		// would be the full line. If we want to use SequenceFile as input, need to
		// set InputFormatClass and input path
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path(input));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(outpath));
		
		
		job.setJarByClass(getClass());
		
		boolean flag = false;
		try {
			//job.submit(); // won't print out executing info
			flag = job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return flag;
	}
	
	public boolean maxViewCountSort(String args[]) throws IOException {
		long ts = System.currentTimeMillis();
		String input = "/data/yelp/";
		String outpath = "/output/yelp/maxViewCountSort_" + ts;
		if(args != null && args.length > 0) {
			input = args[0];
		}
		if(args != null && args.length > 1) {
			outpath = args[1];
		}
		System.out.println("Input path = " + input);
		System.out.println("Output path = " + outpath);
		
		Configuration conf = new Configuration();
		//conf.set("mapred.map.child.log.level", "DEBUG");
		
		Job job = new Job(conf); 
		job.setJobName("YelpViewCountSort_" + ts);
		
		job.setMapperClass(ViewCountSortMapper.class);
		job.setReducerClass(ViewCountSortReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(input));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(outpath));
		
		job.setSortComparatorClass(MyIntComparator.class);
		
		job.setJarByClass(getClass());
		
		boolean flag = false;
		try {
			flag = job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return flag;
	}

	// Use customized comparator to change sort order
	// Need to specify constructor super(IntWritable.class, true);
	// refer: http://doubleenchandra.me/blog/?p=26
	public static class MyIntComparator extends WritableComparator {
		
		public MyIntComparator() {
			super(IntWritable.class, true);
		}
		

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			IntWritable o1 = (IntWritable)a;
			IntWritable o2 = (IntWritable)b;
			return -1 * o1.compareTo(o2);
		}

		
		
	}
	
	public static void main(String[] args) throws IOException {
		YelpDriver driver = new YelpDriver();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		//log.setLevel(Level.DEBUG);
		
		System.out.println("=========STARTED===========");
		long ts = System.currentTimeMillis();
		String tmpFolder = "/output/yelp/tmp_" + ts;
		String dataFolder = "/data/yelp/";
		String outputFolder = "/output/yelp/ReviewCount_" + ts;
		
		System.out.println(fs.getUri());
		if(fs instanceof LocalFileSystem) {
			System.out.println(fs.getName());
			
			String localPrefix = "/Users/xin/hadoop_arena";
			tmpFolder = localPrefix + tmpFolder;
			dataFolder = localPrefix + dataFolder;
			outputFolder = localPrefix + outputFolder;
		}
		
		
		
		String[] countPath = { dataFolder, tmpFolder };
		String[] sortPath = {tmpFolder, outputFolder};
		
		if(driver.maxViewCount(countPath)) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("=========Start Sorting======");
			driver.maxViewCountSort(sortPath);
		}
		
		fs.delete(new Path(tmpFolder), true);
	}

}
