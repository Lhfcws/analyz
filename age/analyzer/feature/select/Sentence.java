package com.yeezhao.analyz.age.analyzer.feature.select;
/**
 * 句子
 * 
 * @author user
 * 
 */
public class Sentence {
	private int sentId;
	private int start;
	private int end;
	public Sentence(int sentId, int start, int end) {
		super();
		this.sentId = sentId;
		this.start = start;
		this.end = end;
	}
	public int getSentId() {
		return sentId;
	}
	public void setSentId(int sentId) {
		this.sentId = sentId;
	}
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	public int getEnd() {
		return end;
	}
	public void setEnd(int end) {
		this.end = end;
	}
	@Override
	public String toString() {
		return "Sentence [sentId=" + sentId + ", start=" + start + ", end="
				+ end + "]";
	}	
}