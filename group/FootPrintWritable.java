package com.yeezhao.analyz.group;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FootPrintWritable extends UserFootPrint implements Writable {
	
	public FootPrintWritable(){
		super();
	}
	
	public FootPrintWritable(String dataType, String publishTime, String content, String id){
		super(dataType, publishTime, content, id);
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("[time:").append(publishTime)
		.append(", type:").append(dataType)
		.append(", content:").append(content)
		.append(", id:").append(id).append("]");
		return sb.toString();
	}
	
	public void readFields(DataInput input) throws IOException {
		dataType = Text.readString(input);
		publishTime = Text.readString(input);
		content = Text.readString(input);
		id = Text.readString(input);
	}

	public void write(DataOutput output) throws IOException {
		Text.writeString(output, dataType);
		Text.writeString(output, publishTime);
		Text.writeString(output, content);
		Text.writeString(output, id);
	}
}
