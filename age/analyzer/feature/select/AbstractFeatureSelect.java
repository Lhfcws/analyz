package com.yeezhao.analyz.age.analyzer.feature.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.yeezhao.analyz.age.analyzer.feature.KeywordType;
import com.yeezhao.analyz.age.analyzer.feature.KeywordTypeSet;
import com.yeezhao.analyz.age.analyzer.weibo.Participle;
import com.yeezhao.analyz.age.analyzer.weibo.Weibo;
import com.yeezhao.analyz.age.analyzer.weibo.WeiboMenagment;

/**
 * 特征选择
 * @author user
 *
 */
abstract public class AbstractFeatureSelect {
	
	public static void main(String[] args) {
		String fileName = "./testData/labelled/cloth_201120214_labelled.txt";
		List<Weibo> weiboList = WeiboMenagment.getWeiboListFormFile(fileName, 2);
		weiboList = weiboList.subList(0, 1);
		List<Participle> keywordList = WeiboMenagment.getKeyWordList(weiboList.get(0));
		List<Participle> sFeatureList = WeiboMenagment.getTextList(weiboList.get(0));
		AbstractFeatureSelect afSelect = new WindowSelect(keywordList, sFeatureList, 4);
		List<Participle> sfList = afSelect.select();

		AbstractFeatureSelect klSelect = new SentenceLengSelect(keywordList, sFeatureList);
		sfList = klSelect.select();

		System.out.println("----------------------");
		System.out.println(weiboList.get(0).getContent());
		for (Participle sf : sfList) {
			System.out.println(String.format("%s, %s, %f, %s, %s", sf.getWord(), sf.getPosTag(), sf.getValue(), sf.getKwTpye(), sf.getfAssignmentType()));
		}
		System.out.println("AbstractFeatureSelect.main()");
	}
	
	protected List<Participle> keywordList;			// 搜索关键词
	protected List<Participle> textPartList;		// 正文的列表
	protected String representWord;					// 代表词
	protected String selectRepresentWord;			// 特征选择的代表词

	// 标点符号集
	final private static Set<String> punctuationSet = new HashSet<String>(
			Arrays.asList("，", ",", "。", "!", "！", "?", "？"));
	
	
	public AbstractFeatureSelect(List<Participle> keywordList,
			List<Participle> sFeatureList) {
		super();
		this.keywordList = keywordList;
		this.textPartList = sFeatureList;
		this.representWord = "myFeature";
	}

	/**
	 * 特征选择
	 * @param keywordList
	 * @param textPartList
	 * @return
	 */
	abstract public List<Participle> select();
	
	/**
	 * 根据关键词内容，返回该关键词在输入列表中的下标位置列表
	 * @param text
	 * @return
	 */
	protected List<Integer> getKeywordIndex(String text) {
		List<Integer> indexList = new ArrayList<Integer>();
		for (int i = 0; i < textPartList.size(); i++) {
			Participle sf = textPartList.get(i);
			if (sf.getWord().equals(text)) {
				indexList.add(i);
			}
		}		
		return indexList;
	}
	/**
	 * 根据关键词类型，返回该关键词在输入列表中的下标位置列表
	 * @param kwType
	 * @return
	 */
	protected List<Integer> getKeywordIndex(KeywordType kwType) {
		List<Integer> indexList = new ArrayList<Integer>();
		for (int i = 0; i < textPartList.size(); i++) {
			Participle sf = textPartList.get(i);
			if (sf.getKwTpye().equals(kwType)) {
				indexList.add(i);
			}
		}		
		return indexList;
	}
	
	/**
	 * 获取代表词
	 * @param text
	 * @return
	 */
	public String getRepresentWord(String text) {
		return representWord + "_" + selectRepresentWord + "_" + text;
	}
	
	/**
	 * 获取句子列表
	 * @param partList
	 * @return
	 */
	public List<Sentence> getSentenceList(List<Participle> partList) {
		List<Sentence> sentList = new ArrayList<Sentence>();
		for (int i = 0; i < partList.size(); i++) {
			Participle p = partList.get(i);
			int start = i;
			while (!punctuationSet.contains(p.getWord())) {
				if (++i < partList.size()) {
					p = partList.get(i);
				} else {
					break;					
				}
			}
			int end = i;
			Sentence s = new Sentence(sentList.size() + 1, start, end);
			sentList.add(s);
		}
		return sentList;
	}
	/**
	 * 获取某一点对应的句子位置
	 * @param sentList
	 * @param index
	 * @return
	 */
	public int getSentenceId(List<Sentence> sentList, int index) {
		for (int i = 0; i < sentList.size(); i++) {
			Sentence s = sentList.get(i);
			if (s.getStart() <= index && s.getEnd() > index) {
				return s.getSentId();
			}
		}
		return -1;
	}
	/**
	 * 设置词的类型
	 */
	protected void setWordType() {
		for (Participle textPart : textPartList) {
			textPart.setKwTpye(KeywordTypeSet.getWordType(textPart.getWord()));
		}
	}
	/**
	 * 获取指定类型词的下标列表
	 * @param kwType
	 * @return
	 */
	protected List<Integer> getWordIndexUseType(KeywordType kwType) {
		List<Integer> indexList = new ArrayList<Integer>();
		for (int i = 0; i < textPartList.size(); i++) {
			Participle textPart = textPartList.get(i);
			if (textPart.getKwTpye() == kwType) {
				indexList.add(i);
			}
		}
		return indexList;
	}
	/**
	 * 返回左/右窗口的子序列
	 * @param list 原列表
	 * @param curIndex 当前点位置
	 * @param isLeft 左边=true,右边=false
	 * @param size 子序列长度
	 * @return
	 */
	public static <T> List<T> getSubList(List<T> list, Integer curIndex, int size, Boolean isLeft) {
		List<T> subList = new ArrayList<T>();		
		int fromIndex = isLeft ? Math.max(curIndex - size, 0) : curIndex;
		int toIndex = isLeft ? curIndex : Math.min(curIndex + size, list.size());
		if (fromIndex >= 0 && toIndex <= list.size() && fromIndex < toIndex) {
			subList.addAll(list.subList(fromIndex, toIndex));
		}
		return subList;
	}
}
