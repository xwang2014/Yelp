package yelp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Yelp {
	
	public void encodeJson() {
		  JSONObject obj=new JSONObject();
		  
		  obj.put("name","foo");
		  obj.put("num",new Integer(100));
		  obj.put("balance",new Double(1000.21));
		  obj.put("is_vip",new Boolean(true));
		  obj.put("nickname",null);
		  System.out.print(obj);		
	}
	
	public void readJson() {
		BufferedReader br = null;
		String jsonFile = "/Users/xin/dataset/yelp/yelp_business_4.json";
		try {
			br = new BufferedReader(new FileReader(jsonFile));
			
			JSONParser parser = new JSONParser();
			String line = "";
			int cnt = 0;
			while((line = br.readLine()) != null) {
				System.out.println("LINE =  " + line);
				JSONObject obj = (JSONObject) parser.parse(line);
				System.out.println(obj.toJSONString());
				
				cnt++;
				if(cnt == 2) break;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Yelp y = new Yelp();
		y.readJson();
	}

}
