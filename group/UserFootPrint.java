package com.yeezhao.analyz.group;

public class UserFootPrint {
	protected String dataType;
	protected String publishTime; //如果没有发表时间，用空字符串而非null表示
	protected String content;
	protected String id; //如果没有id，用空字符串而非null表示
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getPublishTime() {
		return publishTime;
	}

	public void setPublishTime(String publishTime) {
		this.publishTime = publishTime;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}
	
	public UserFootPrint(){}
	
	public UserFootPrint(String dataType, String publishTime, String content, String id){
		this.dataType = dataType;
		this.publishTime = publishTime;
		this.content = content;
		this.id = id;
	}
	
	public UserFootPrint(UserFootPrint fp){
		this(fp.getDataType(), fp.getPublishTime(), fp.getContent(), fp.getId());
	}
}
