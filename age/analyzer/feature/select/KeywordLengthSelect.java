package com.yeezhao.analyz.age.analyzer.feature.select;

import java.util.ArrayList;
import java.util.List;

import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;
import com.yeezhao.analyz.age.analyzer.weibo.Participle;

/**
 * b.搜索词的字个数 （个数除以10）
 * @author user
 *
 */
public class KeywordLengthSelect extends AbstractFeatureSelect {

	
	public KeywordLengthSelect(List<Participle> keywordList,
			List<Participle> sFeatureList) {
		super(keywordList, sFeatureList);
		this.selectRepresentWord = "kwLeng";
	}

	@Override
	public List<Participle> select() {
		List<Participle> selectList = new ArrayList<Participle>();
		
		int sumLength = 0;
		for (Participle kw : keywordList) {
			sumLength += kw.getWord().length();
		}
		String title = "leng_";
		double value = keywordList.size() > 0 ? (double)sumLength / keywordList.size() : 0;	// 平均长度
		value /= 4;
		if (value > 1) {	// 设置上限
			value = 1;
		}
		String text = getRepresentWord(title);
		Participle sf = new Participle(text);
		sf.setValue(value);
		sf.setfAssignmentType(AssignmentType.DIRECT);
		selectList.add(sf);		
		return selectList;
	}

}
