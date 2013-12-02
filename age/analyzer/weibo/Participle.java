package com.yeezhao.analyz.age.analyzer.weibo;

import com.yeezhao.analyz.age.analyzer.feature.KeywordType;
import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;

/**
 * 微博的分词
 * @author user
 *
 */
public class Participle {

	private String word; 			// 词组
	private double value;			// 特征的值
	
	private String posTag;			// 词性标注
	private KeywordType kwTpye;		// 关键词类型

	private AssignmentType fAssignmentType;	// 特征赋值类型

	/**
	 * 部分的克隆
	 * @return
	 */
	public Participle partClone() {
		Participle part = new Participle(this.word);
		part.setPosTag(this.posTag);
		part.setKwTpye(this.kwTpye);
		return part;
	}
	
	public Participle(String word) {
		this.word = word;
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	public String getPosTag() {
		return posTag;
	}
	public void setPosTag(String posTag) {
		this.posTag = posTag;
	}
	public KeywordType getKwTpye() {
		return kwTpye;
	}
	public void setKwTpye(KeywordType kwTpye) {
		this.kwTpye = kwTpye;
	}
	public AssignmentType getfAssignmentType() {
		return fAssignmentType;
	}
	public void setfAssignmentType(AssignmentType fAssignmentType) {
		this.fAssignmentType = fAssignmentType;
	}
}
