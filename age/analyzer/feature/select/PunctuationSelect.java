package com.yeezhao.analyz.age.analyzer.feature.select;

import java.util.ArrayList;
import java.util.List;

import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;
import com.yeezhao.analyz.age.analyzer.weibo.Participle;

/**
 * d. 搜索词之间是否有标点符号（1表示有，0表示没有）
 * @author user
 *
 */
public class PunctuationSelect extends AbstractFeatureSelect {

	public PunctuationSelect(List<Participle> keywordList, List<Participle> sFeatureList) {
		super(keywordList, sFeatureList);
		this.selectRepresentWord = "punct";
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

		int count = 0;
		int dictSum = 0;
		for (int i = 0; i < keywordIndexList.size(); i++) {
			List<Integer> list1 = keywordIndexList.get(i);
			for (int j = i + 1; j < keywordIndexList.size(); j++) {
				List<Integer> list2 = keywordIndexList.get(j);
				// 计算 句子距离绝对值
				for (Integer index1 : list1) {
					for (Integer index2 : list2) {
						int sentId1 = getSentenceId(sentList, index1);
						int sentId2 = getSentenceId(sentList, index2);
						if (sentId1 >= 0 && sentId2 >= 0) {
							int dict = Math.abs(sentId1 - sentId2);
							dictSum += dict;
							count++;
						}
						
					}
				}
			}
		}

		String title = "avg";
		double value = count > 0 ? (double)dictSum / count : 0;	// 平均长度
		value /= 50;
		if (value > 1) {	// 设置上限
			value = 1;
		}
	//	value = count > 0 ? 1 : 0;
		String text = getRepresentWord(title);
		Participle sf = new Participle(text);
		sf.setValue(value);
		sf.setfAssignmentType(AssignmentType.DIRECT);
		selectList.add(sf);
		
		return selectList;
	}
}

