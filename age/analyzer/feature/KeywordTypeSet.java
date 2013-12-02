package com.yeezhao.analyz.age.analyzer.feature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.yeezhao.analyz.age.analyzer.utility.FileHandler;

/**
 * 
 * @author user
 *
 */
public class KeywordTypeSet {

	final private static String clothPath = "./clth/knowledge/";
	private static Map<KeywordType, Set<String>> kwTypeSetMap;
	private static Map<String, KeywordType> word2typeMap;
	static {
		kwTypeSetMap = new HashMap<KeywordType, Set<String>>();
		kwTypeSetMap.put(KeywordType.NEG_SENT, getWordSet(clothPath + "neg-sentiments.txt"));
		kwTypeSetMap.put(KeywordType.POS_SENT, getWordSet(clothPath + "pos-sentiments.txt"));
		kwTypeSetMap.put(KeywordType.TYPE, getWordSet(clothPath + "filter-types.txt"));
		kwTypeSetMap.put(KeywordType.BRAND, getWordSet(clothPath + "goo5-cloth-brands.txt"));
		
		word2typeMap = new HashMap<String, KeywordType>();
		for (Map.Entry<KeywordType, Set<String>> entry : kwTypeSetMap.entrySet()) {
			List<String> wordList = new ArrayList<String>(entry.getValue());
			for (String word : wordList) {
				word2typeMap.put(word, entry.getKey());
			}
		}
	}
	/**
	 * 获取词库对应的类型
	 * @param text
	 * @return
	 */
	public static KeywordType getWordType(String text) {
		KeywordType kwType = word2typeMap.get(text);
		if (kwType == null) {
			kwType = KeywordType.UNKNOW;
		}
		return kwType;
	}
	/**
	 * 获取词库
	 * @param wordSetFileName 词库文件名
	 * @return
	 */
	private static Set<String> getWordSet(String wordSetFileName) {	
		List<String> wordList = FileHandler.readFileToList(wordSetFileName);		
		Set<String> wordSet = new HashSet<String>();
		wordSet.addAll(wordList);
		return wordSet;
	}
}
