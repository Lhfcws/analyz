package com.yeezhao.analyz.age.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import weka.core.Attribute;
import weka.core.FastVector;

public class AttributeTool {

	Map<String, Integer> indexMap = new HashMap<String, Integer>();

	public AttributeTool(FastVector attributes) {
		super();
		this.indexMap = getInitIndexMap(attributes);
	}
	private Map<String, Integer> getInitIndexMap(FastVector attributes) {
		Map<String, Integer> indexMap = new HashMap<String, Integer>();	
		List<String> wordList = new ArrayList<String>();
		for (int i = 0; i < attributes.size(); i++) {
			Attribute attri = (Attribute)(attributes.elementAt(i));
			wordList.add(attri.name());
		}		
		Integer[] sampleCounts = new Integer[wordList.size()];
		Arrays.fill(sampleCounts, 0);
		int count = 0;
		for (String word : wordList) {
			indexMap.put(word, count);
			count++;
		}
		return indexMap;
	}
	public Integer getIndex(String labelString) {
		return indexMap.get(labelString);
	}
	public int getLabelCount() {
		return indexMap.size();
	}
}
