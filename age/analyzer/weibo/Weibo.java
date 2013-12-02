package com.yeezhao.analyz.age.analyzer.weibo;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;

/**
 * 已建立标签的微博
 * 
 * @author yunbiao
 * 
 */
public class Weibo {
	
    private int ID;							// ID
	private String keywords; 				// 关键字
	private String content; 				// 正文内容
	private String label; 					// 标签,("1"=第一类，负类),("2"=第一类，负类),("3"=第二类，正类)
	private CATEGORY_TYPE categoryType; 	// 分类类型,(1=第一类，负类),(2=第二类，正类)
	private List<String> segmentedList; 	// 分词
	private List<WeiboFeature> featureList;	// 特征列表
//	private int classifierNum;				// 针对分类，谁是正类
	private String simHashCode;				// simHash,用于文本去重

	private String firstKeyword;			// 第一关键词
	private String secondKeyword;			// 第二关键词
	private String thirdKeyword;			// 第三关键词

	private List<Participle> textPartList;	// 单元列表

	public List<String> getAllSegmentedList() {
		List<String> allSegmentedList = new ArrayList<String>();
		allSegmentedList.addAll(this.segmentedList);
		List<Participle> pList = getPartList(AssignmentType.TFIDF);
		for (Participle p : pList) {
			allSegmentedList.add(p.getWord());
		}		
		return allSegmentedList;
	}
	/**
	 * 根据赋值类型，获取相应的分词
	 * @param assignmentType
	 * @return
	 */
	public List<Participle> getPartList(AssignmentType assignmentType) {
		List<Participle> pList = new ArrayList<Participle>();
		if (textPartList != null) {				
			for (Participle p : textPartList) {
				if (p.getfAssignmentType() == assignmentType) {
					pList.add(p);
				}
			}
		}
		return pList;
	}

	public static int cagegoryTypeToInt(CATEGORY_TYPE type)	{
		return type == CATEGORY_TYPE.NEG ? 1 : 2;
	}
	public Weibo(String content) {
		categoryType = CATEGORY_TYPE.NEG;	//不知道类型的情况下默认负类
		this.content = content;
	}
	/**
	 * 
	 * @param keyWords
	 * @param content
	 * @param label
	 * @param classifierNum
	 */
	public Weibo(String keywords, String content, String label, int id, int classifierNum) {
		label = label.trim();
		this.keywords = keywords;
		this.content = content;
		this.label = label.trim();
		this.ID = id;
		int category = getClassNum();
		
		segmentedList = new ArrayList<String>();
		textPartList = new ArrayList<Participle>();

		if (category >= 0 && category <= 3) {
			categoryType = (classifierNum == category) ? CATEGORY_TYPE.POS : CATEGORY_TYPE.NEG;	// true为正类，false为负类
		}else {
			System.err.println("出现没有的分类！ " + label);
		}
		cutKeywords();
	}
	
	/**
	 * 分割关键词
	 */
	public void cutKeywords() {
		if (keywords != null) {
			Pattern pattern = Pattern.compile("[ |]+");
			String[] split = pattern.split(keywords);
			firstKeyword = (split.length > 0)? split[0] : "";
			secondKeyword = (split.length > 1)? split[1] : "";
			thirdKeyword = (split.length > 2)? split[2] : "";
		}
	}

	public void addTextPartList(List<Participle> textPartList) {
		if (this.textPartList == null) {
			textPartList = new ArrayList<Participle>();
		}
		this.textPartList.addAll(textPartList);
	}
	public void addPart(Participle part) {
		if (this.textPartList == null) {
			textPartList = new ArrayList<Participle>();
		}
		this.textPartList.add(part);
	}

	/**
	 * 获取类别号码，0、1、2或3，-1时是错误类型
	 * @return
	 */
	public int getClassNum() {
		return getClassNum(this.label);
	}
	public static int getClassNum(String label) {
		if (label == null || label.length() == 0) {
			System.err.println("没有类型:" + label);
			return -1;
		}
		double d = Double.parseDouble(label);
	/*	if (d >=1 && d <= 1.1) {
			return 3;
		} else if (d >= 0 && d < 3 ){
			return (int)d;
		}  else if (d >= 3 && d < 4 ){
			return (int)d;
		} else {
			System.err.println("没有类型:" + label);
			return -1;
		}*/
		int num = (int)d;
		if (num >=0 && num <= 3) {
			return num;
		} else {
			System.err.println("没有类型:" + label);
			return -1;
		}
	}
	public List<String> getSegmented() {
		return segmentedList;
	}

	public void setSegmented(List<String> segmentedList) {
		this.segmentedList = segmentedList;
	}
	/**
	 * 字符串形式返回类别
	 * @return "1"=负，"2"=正
	 */
	public String getCategoryTypeToString() {
		return categoryType == CATEGORY_TYPE.NEG ? "1" : "2";
	}	
	public CATEGORY_TYPE getCategoryType() {
		return categoryType;
	}
	public void setCategoryID(CATEGORY_TYPE categoryType) {
		this.categoryType = categoryType;
	}
	public String getKeyWords() {
		return keywords;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public void setKeyWords(String keyWords) {
		this.keywords = keyWords;
	}	
	public List<WeiboFeature> getFeatureList() {
		return featureList;
	}
	public void setFeatureList(List<WeiboFeature> featureList) {
		this.featureList = featureList;
	}
	public int getID() {
		return ID;
	}
	public String getSimHashCode() {
		return simHashCode;
	}
	public void setSimHashCode(String simHashCode) {
		this.simHashCode = simHashCode;
	}	
	public String getFirstKeyword() {
		return firstKeyword;
	}
	public void setFirstKeyword(String firstKeyword) {
		this.firstKeyword = firstKeyword;
	}
	public String getSecondKeyword() {
		return secondKeyword;
	}
	public void setSecondKeyword(String secondKeyword) {
		this.secondKeyword = secondKeyword;
	}
	public String getThirdKeyword() {
		return thirdKeyword;
	}
	public void setThirdKeyword(String thirdKeyword) {
		this.thirdKeyword = thirdKeyword;
	}
	public List<Participle> getTextPartList() {
		return textPartList;
	}
	public void setTextPartList(List<Participle> textPartList) {
		this.textPartList = textPartList;
	}

	/**
	 * 分类类型
	 * 
	 * @author user
	 * 
	 */
	public enum CATEGORY_TYPE {
		NEG,	// 负类
		POS		// 正类
	};
}
