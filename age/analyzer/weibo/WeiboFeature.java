package com.yeezhao.analyz.age.analyzer.weibo;

/**
 * 微博特征，赋值时用
 * @author user
 *
 */
public class WeiboFeature {

	private String word; 		// 分词
	private int ID; 			// 该特征的ID
	private double value;		// 特征的值
	

	public WeiboFeature(String word) {
		this.word = word;
	}
	/**
	 * 构造方法，输出时调用
	 * 
	 * @param word
	 * @param ID
	 * @param value
	 */
	public WeiboFeature(String word, int ID, double value) {
		super();
		this.word = word;
		this.ID = ID;
		this.value = value;
	}
	
	
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public int getID() {
		return ID;
	}
	public void setID(int iD) {
		ID = iD;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	
	
}
