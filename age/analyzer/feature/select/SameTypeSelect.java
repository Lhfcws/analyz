package com.yeezhao.analyz.age.analyzer.feature.select;

import java.util.ArrayList;
import java.util.List;

import com.yeezhao.analyz.age.analyzer.feature.KeywordType;
import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;
import com.yeezhao.analyz.age.analyzer.weibo.Participle;

/**
 * e. 搜索词之间是否还有同样kw-type词（1表示有，0表示没有）
 * @author user
 *
 */
public class SameTypeSelect extends AbstractFeatureSelect {

	
	public SameTypeSelect(List<Participle> keywordList,
			List<Participle> sFeatureList) {
		super(keywordList, sFeatureList);
		this.selectRepresentWord = "sameType";
	}

	@Override
	public List<Participle> select() {
		setWordType();
		
		List<Participle> selectList = new ArrayList<Participle>();
		
		// 关键词下标位置
		List<List<Integer>> keywordIndexList = new ArrayList<List<Integer>>();
		for (Participle sf : keywordList) {
			keywordIndexList.add(getKeywordIndex(sf.getWord()));
		}				
		
		int count = 0;
		for (int i = 0; i < keywordIndexList.size(); i++) {
			List<Integer> list1 = keywordIndexList.get(i);			
			for (int j = i + 1; j < keywordIndexList.size(); j++) {
				List<Integer> list2 = keywordIndexList.get(j);
				
				// 计算 句子距离绝对值
				for (Integer index1 : list1) {
					KeywordType kwt1 = textPartList.get(index1).getKwTpye();
					for (Integer index2 : list2) {
					KeywordType kwt2 = textPartList.get(index2).getKwTpye();
						int minIndex = Math.min(index1, index2);
						int maxIndex = Math.max(index1, index2);
						for (int k = minIndex + 1; k < maxIndex; k++) {
							Participle p = textPartList.get(k);
							if (p.getKwTpye() == kwt1 || p.getKwTpye() == kwt2) {
								count++;
							}
						}			
					}
				}
			}
		}

		String title = "avg";
		double value = count > 0 ? 1 : 0;
/*		if (value > 1) {	// 设置上限
			value = 1;
		}*/
		String text = getRepresentWord(title);
		Participle sf = new Participle(text);
		sf.setValue(value);
		sf.setfAssignmentType(AssignmentType.DIRECT);
		selectList.add(sf);
		
		return selectList;
	}

}
