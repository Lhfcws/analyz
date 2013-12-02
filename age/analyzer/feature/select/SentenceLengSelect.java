package com.yeezhao.analyz.age.analyzer.feature.select;

import java.util.ArrayList;
import java.util.List;

import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;
import com.yeezhao.analyz.age.analyzer.weibo.Participle;

/**
 * f. 句子的长度（ 如果跨句子，将所跨句子长度相加）（句子长度除以140）
 * @author user
 *
 */
public class SentenceLengSelect extends AbstractFeatureSelect {

	public SentenceLengSelect(List<Participle> keywordList,
			List<Participle> sFeatureList) {
		super(keywordList, sFeatureList);
		this.selectRepresentWord = "sentLeng";
	}

	@Override
	public List<Participle> select() {

		List<Participle> selectList = new ArrayList<Participle>();
		
		// 关键词下标位置
		List<List<Integer>> keywordIndexList = new ArrayList<List<Integer>>();
		for (Participle sf : keywordList) {
			keywordIndexList.add(getKeywordIndex(sf.getWord()));
		}				
		List<Sentence> sentList = getSentenceList(textPartList);	// 句子区间	

		int maxId = Integer.MIN_VALUE;
		int minId = Integer.MAX_VALUE;
		for (List<Integer> kwList : keywordIndexList) {
			for (Integer index : kwList) {
				int sentId = getSentenceId(sentList, index);
				maxId = Math.max(sentId, maxId);
				minId = Math.min(sentId, minId);
			}
		}
		
		int sentLeng = 0;
		if (maxId >= 0 && minId >= 0 && maxId <= sentList.size() && minId <= sentList.size() && minId <= maxId) {
			Sentence minSent = sentList.get(minId - 1);
			Sentence maxSent = sentList.get(maxId - 1);
			sentLeng = maxSent.getEnd() - minSent.getStart();
		}

		String title = "leng";
		double value = sentLeng;	// 平均长度
		value /= 140;
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
