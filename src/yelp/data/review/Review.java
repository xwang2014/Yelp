package yelp.data.review;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.json.simple.JSONObject;

public class Review implements Writable {
	
	private static final short CODE_VERSION = 1;
	
	public String reviewId;
	public String businessId;
	public String userId;
	public long stars;
	public String date;
	public String text;
	public long funnyVote = 0;
	public long usefulVote = 0;
	public long coolVote = 0;
	


	@Override
	public void readFields(DataInput input) throws IOException {
		short version = input.readShort();
		if(version > CODE_VERSION) {
			try {
				throw new Exception("Code version is wrong!");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		switch(version) {
			case 1:
				this.reviewId = input.readUTF();
				this.businessId = input.readUTF();
				this.userId = input.readUTF();
				this.stars = input.readLong();
				this.date = input.readUTF();
				this.text = input.readUTF();
				this.funnyVote = input.readLong();
				this.usefulVote = input.readLong();
				this.coolVote = input.readLong();
				break;
				
			default:
				break;
		}
		
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeShort(CODE_VERSION);
		output.writeUTF(reviewId);
		output.writeUTF(businessId);
		output.writeUTF(userId);
		output.writeLong(stars);
		output.writeUTF(date);
		output.writeUTF(text);
		output.writeLong(funnyVote);
		output.writeLong(usefulVote);
		output.writeLong(coolVote);
	}
	
	public static Review readFromJson(JSONObject obj) {
		Review review = new Review();
		
		review.reviewId = (String) obj.get("review_id");
		review.businessId = (String) obj.get("business_id");
		review.userId = (String) obj.get("user_id");
		review.stars = (Long) obj.get("stars");
		review.date = (String) obj.get("date");
		review.text = (String) obj.get("text");
		review.funnyVote = ((Map<String, Long>)obj.get("votes")).get("funny");
		review.usefulVote =  ((Map<String, Long>)obj.get("votes")).get("useful");
		review.coolVote = ((Map<String, Long>)obj.get("votes")).get("cool");
		
		return review;
	}

}
