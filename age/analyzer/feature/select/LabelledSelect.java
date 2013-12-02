package com.yeezhao.analyz.age.analyzer.feature.select;

import java.util.ArrayList;
import java.util.List;

import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;
import com.yeezhao.analyz.age.analyzer.weibo.Participle;

public class LabelledSelect extends AbstractFeatureSelect {

	private String label;
	public LabelledSelect(List<Participle> keywordList,
			List<Participle> sFeatureList, String label) {
		super(keywordList, sFeatureList);
		this.selectRepresentWord = "label";
		this.label = label;
	}

	@Override
	public List<Participle> select() {

		List<Participle> selectList = new ArrayList<Participle>();
		
		
		Participle sf = new Participle("label");
	//	double value = Double.valueOf(label) / 10;
		double value = label.equals("2") ? 1 : 0;
		sf.setValue(value);
		sf.setfAssignmentType(AssignmentType.DIRECT);
		selectList.add(sf);	
		
		return selectList;
	}

}
