package com.yeezhao.analyz.age.data;

import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import weka.core.Attribute;

/**
 * 分类标签
 * @author Administrator
 *
 */
public class Label {

	Map<String, Integer> indexMap = new HashMap<String, Integer>();
	Map<Integer, String> labelMap = new HashMap<Integer, String>();

	public Label(Attribute labels) {
		super();
		this.indexMap = getInitIndexMap(labels);
	}
	private Map<String, Integer> getInitIndexMap(Attribute labels) {
		Map<String, Integer> indexMap = new HashMap<String, Integer>();
		Set<String> labelSet = new TreeSet<String>();
		for (@SuppressWarnings("unchecked")
		Enumeration<String> e = labels.enumerateValues(); e.hasMoreElements();) {
			labelSet.add((String)(e.nextElement()));
		}
		Integer[] sampleCounts = new Integer[labelSet.size()];
		Arrays.fill(sampleCounts, 0);
		int count = 0;
		labelMap = new HashMap<Integer, String>();
		for (String label : labelSet) {
			indexMap.put(label, count);
			labelMap.put(count, label);
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
	public String getLabel(int index) {
		return labelMap.get(index);
	}
}
