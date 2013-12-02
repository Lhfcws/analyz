package com.yeezhao.analyz.age.comparator;

import java.util.Comparator;

import com.yeezhao.analyz.age.analyzer.utility.CompareValue;

public class StringHashCmp implements Comparator<String> {
	public int compare(String obj1, String obj2) {
		return CompareValue.acs(obj1.hashCode(), obj2.hashCode());	//升序
	}
}
