package yelp.data.business;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.json.simple.JSONObject;

public class Business implements Writable {
	
	private static final short CODE_VERSION = 1;
	
	public String businessId;
	public String name;
	public String fullAddress;
	public String city;
	public String state;
	public double latitude;
	public double longitude;
	public long reviewCount;
	

	@Override
	public void readFields(DataInput input) throws IOException {
		short codeVersion = input.readShort();
		if(codeVersion > CODE_VERSION) {
			try {
				throw new Exception("Code version is wrong!");
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
		}
		
		switch(codeVersion) {
			case 1:
				this.businessId = input.readUTF();
				this.name = input.readUTF();
				this.fullAddress = input.readUTF();
				this.city = input.readUTF();
				this.state = input.readUTF();
				this.latitude = input.readLong();
				this.longitude = input.readLong();
				this.reviewCount = input.readLong();
				return;
				
			default:
				return;	
		}
		
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeShort(CODE_VERSION);
		output.writeUTF(businessId);
		output.writeUTF(name);
		output.writeUTF(fullAddress);
		output.writeUTF(city);
		output.writeUTF(state);
		output.writeDouble(latitude);
		output.writeDouble(longitude);
		output.writeLong(reviewCount);
	}
	
	public static Business readFromJson(JSONObject obj) {
		Business bus = new Business();
		bus.businessId = (String) obj.get("business_id");
		bus.name = (String) obj.get("name");
		bus.fullAddress = (String) obj.get("full_address");
		bus.city = (String) obj.get("city");
		bus.state = (String) obj.get("state");
		bus.latitude = (Double) obj.get("latitude");
		bus.longitude = (Double) obj.get("longitude");
		bus.reviewCount = (Long) obj.get("review_count");
		
		return bus;
	}


	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
