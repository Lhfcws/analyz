package com.yeezhao.analyz.age.comparator;

import java.util.Comparator;

import com.yeezhao.analyz.age.analyzer.utility.CompareValue;
import com.yeezhao.analyz.age.data.AttributeFeature;


public class AttributeFeatureCHICmp implements Comparator<AttributeFeature> {
	public int compare(AttributeFeature obj1, AttributeFeature obj2) {
		return CompareValue.desc(obj1.getCHI(), obj2.getCHI());		//降序
	}
}