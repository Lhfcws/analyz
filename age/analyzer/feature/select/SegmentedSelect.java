package com.yeezhao.analyz.age.analyzer.feature.select;

import java.util.ArrayList;
import java.util.List;

import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;
import com.yeezhao.analyz.age.analyzer.weibo.Participle;

/**
 * 文本分词选择，最基本的
 * @author user
 *
 */
public class SegmentedSelect extends AbstractFeatureSelect {

	public SegmentedSelect(List<Participle> keywordList,
			List<Participle> sFeatureList) {
		super(keywordList, sFeatureList);
		this.selectRepresentWord = "segmented";
	}

	@Override
	public List<Participle> select() {
		List<Participle> selectList = new ArrayList<Participle>();
		for (Participle part : textPartList) {
			Participle p = part.partClone();
			p.setfAssignmentType(AssignmentType.TFIDF);
			selectList.add(p);
		}
		return selectList;
	}

}
