package com.yeezhao.analyz.age.analyzer.feature.select;

import java.util.ArrayList;
import java.util.List;

import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;
import com.yeezhao.analyz.age.analyzer.weibo.Participle;

/**
 * c. 搜索词和搜索词的距离（距离除以500）
 * @author user
 *
 */
public class KeywordDistSelect extends AbstractFeatureSelect {

	public KeywordDistSelect(List<Participle> keywordList, List<Participle> sFeatureList) {
		super(keywordList, sFeatureList);
		this.selectRepresentWord = "kwDist";
	}

	@Override
	public List<Participle> select() {
		List<Participle> selectList = new ArrayList<Participle>();
		
		// 关键词下标位置
		List<List<Integer>> keywordIndexList = new ArrayList<List<Integer>>();
		for (Participle sf : keywordList) {
			keywordIndexList.add(getKeywordIndex(sf.getWord()));
		}
		
		int count = 0;
		int dictSum = 0;
		for (int i = 0; i < keywordIndexList.size(); i++) {
			List<Integer> list1 = keywordIndexList.get(i);
			for (int j = i + 1; j < keywordIndexList.size(); j++) {
				List<Integer> list2 = keywordIndexList.get(j);
				// 计算 距离绝对值
				for (Integer index1 : list1) {
					for (Integer index2 : list2) {
						int dict = Math.abs(index1 - index2);
						dictSum += dict;
						count ++;
			//			System.out.println(index1 + "\t" + index2 + "\t" + dict + "\t" + (dictSum / count));
					}
				}
			}
		}
/*		System.out.println(count + "=============");
		if (count < 1) {
			System.out.println(Arrays.asList(keywordList) + "\t" + Arrays.asList(textPartList));
			System.err.println("-----------------");
		}
		if (count > 1) {
			System.err.println("+++++++++++++++++++");
		}
		*/
		String title = "avg";
		double value = count > 0 ? (double)dictSum / count : 0;	// 平均长度
	//	value /= 100;
		value /= 50;
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
