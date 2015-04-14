package yelp.data.business;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Dataset {
	Configuration conf;
	FileSystem dfs = null;
	
	public Dataset() {
		conf = new Configuration();
	}
	
	public void createDataFile(String rawData, String outFile) {
		// init dfs
		try {
			dfs = FileSystem.get(new URI("hdfs://localhost:9000/data"), conf);
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
		//
		Path newPath = null, tmpPath = null;
		BufferedReader br = null;
		SequenceFile.Writer writer = null;
		try {
			System.out.println(dfs.getUri());
			System.out.println(outFile);
			
			br = new BufferedReader(new FileReader(rawData));
			long current_ts = System.currentTimeMillis();
			newPath = new Path(outFile);
			tmpPath = new Path(outFile + "." + current_ts);
			
			Text key = new Text();
			Business value = new Business();
			
			writer = SequenceFile.createWriter(dfs, conf, tmpPath, 
					key.getClass(), value.getClass());
			
			JSONParser parser = new JSONParser();
			String line = "";
			int cnt = 0;
			while((line = br.readLine()) != null) {
				JSONObject obj = (JSONObject) parser.parse(line);
				value = Business.readFromJson(obj);
				key = new Text(value.businessId);
				
				writer.append(key, value);
				cnt++;
				
			}
			
			System.out.println("Total line number = " + cnt);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				//writer.hflush();
				writer.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}

		boolean flag = false;
		try {
			if(dfs.exists(newPath)) {
				dfs.delete(newPath, true);
			}
			flag = dfs.rename(tmpPath, newPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void readBusiness(String file) {
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		SequenceFile.Reader reader = null;
		
		try {
			reader = new SequenceFile.Reader(fs, new Path(file), conf);
			
			Text key = new Text();
			Business value = new Business();
			
			int cnt = 0;
			while(reader.next(key, value)) {
				cnt++;
				if(cnt % 100 == 0) {
					System.out.println(key + "  -  "  + value.name + "  - " + value.reviewCount);
				}
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		String rawData = "/Users/xin/dataset/yelp/yelp_business_1.json";
		String outFile = "/data/yelp/yelp_business_1.json";
		
		Dataset ds = new Dataset();
		
		for(int i = 1; i <= 4; i++) {
			rawData = "/Users/xin/dataset/yelp/yelp_business_" + i + ".json";
			outFile = "/data/yelp/yelp_business_"+ i + ".json";
			ds.createDataFile(rawData, outFile);
		}
		
		//ds.readBusiness(outFile);
	}

}
