package com.yeezhao.analyz.age.analyzer.feature.select;

import java.util.ArrayList;
import java.util.List;

import com.yeezhao.analyz.age.analyzer.feature.KeywordType;
import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;
import com.yeezhao.analyz.age.analyzer.weibo.Participle;

/**
 * g. 情感词窗口内是否存在其他情感词 （周围最近的情感词的距离）（距离除以情感词窗口值）／／如果有情感词
 * @author user
 *
 */
public class SentimentSelect extends AbstractFeatureSelect {

	private int windowSize;	// 窗口大小
	
	public SentimentSelect(List<Participle> keywordList,
			List<Participle> sFeatureList, int windowSize) {
		super(keywordList, sFeatureList);
		this.selectRepresentWord = "sentiment";
		this.windowSize = windowSize;
	}

	@Override
	public List<Participle> select() {

		setWordType();
		
		List<Participle> selectList = new ArrayList<Participle>();
		
		List<Integer> sentimentList = getWordIndexUseType(KeywordType.NEG_SENT);
		
		int count = 0;
		int sumLeng = 0;
		for (int i = 0; i < sentimentList.size(); i++) {
			int index1 = sentimentList.get(i);
			for (int j = i + 1; j < sentimentList.size(); j++) {
				int index2 = sentimentList.get(j);
				int leng = Math.abs(index2 - index1);
				if (leng <= windowSize) {
					sumLeng += leng;
					count++;
				} else {
					break;
				}
			}
		}
		
		String title = "leng";
		double value = count > 0 ? (double)windowSize / count : 0;
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

	public int getWindowSize() {
		return windowSize;
	}
	public void setWindowSize(int windowSize) {
		this.windowSize = windowSize;
	}

	
}
