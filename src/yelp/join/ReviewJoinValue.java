package yelp.join;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.Writable;

import yelp.data.business.Business;

public class ReviewJoinValue implements Writable {
	
	public static final short CODE_VERSION = 1;
	
	public String businessId = "";
	public boolean isBusData;
	
	public Business busData;

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
			this.isBusData = input.readBoolean();
			boolean hasBusData = input.readBoolean();
			if(hasBusData) {
				this.busData = new Business();
				this.busData.readFields(input);
			} else {
				this.busData = null;
			}
			break;	
			
			default:
				break;
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeShort(CODE_VERSION);
		
		output.writeUTF(businessId);
		output.writeBoolean(isBusData);
		if(this.busData != null) {
			output.writeBoolean(true);
			this.busData.write(output);
		} else {
			output.writeBoolean(false);
		}
	}
	
}
