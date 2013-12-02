package com.yeezhao.analyz.age.analyzer.feature.select;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.yeezhao.analyz.age.analyzer.feature.KeywordType;
import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;
import com.yeezhao.analyz.age.analyzer.weibo.Participle;

/**
 * h. 搜索词（假如该搜索词不是情感词的话）周围（一定距离限制内）的情感词数目 （该距离限制通常要比窗口大小大）（情感词数目除以两倍窗口值）
 * @author user
 *
 */
public class KeywordSentimentSelect extends AbstractFeatureSelect {

	private int windowSize;	// 窗口大小	
	
	public KeywordSentimentSelect(List<Participle> keywordList,
			List<Participle> sFeatureList, int windowSize) {
		super(keywordList, sFeatureList);
		this.windowSize = windowSize;
		this.selectRepresentWord = "keywordSenti";
	}

	@Override
	public List<Participle> select() {
		setWordType();
		
		List<Participle> selectList = new ArrayList<Participle>();
		

		List<Integer> sentimentList = getWordIndexUseType(KeywordType.NEG_SENT);	// 情感词
		List<Integer> notSentimentList = new ArrayList<Integer>();						//  非情感词
		
		Set<KeywordType> kwTypeSet = getKeywordTypeSet();
		if (kwTypeSet.contains(KeywordType.NEG_SENT)) {
			kwTypeSet.remove(KeywordType.NEG_SENT);
		}
		for (KeywordType kwType : kwTypeSet) {
			notSentimentList.addAll(getWordIndexUseType(kwType));
		}
		
		int count = 0;
		int sumLeng = 0;
		
		for (int i = 0; i < notSentimentList.size(); i++) {
			int index1 = notSentimentList.get(i);
			for (int j = 0; j < sentimentList.size(); j++) {
				int index2 = sentimentList.get(j);
				int leng = Math.abs(index2 - index1);	// 非情感词和情感词距离
				if (leng <= windowSize * 2) {
					sumLeng += leng;
					count++;
				} else {
					break;
				}
			}
		}
		
		String title = "leng";
		double value = count > 0 ? (double)windowSize / count / 2 : 0;
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

	/**
	 * 获取正文词的类型
	 * @return
	 */
	private Set<KeywordType> getKeywordTypeSet() {
		Set<KeywordType> kwTypeSet = new HashSet<KeywordType>();
		for (Participle sf : textPartList) {
			kwTypeSet.add(sf.getKwTpye());
		}		
		return kwTypeSet;
	}

	public int getWindowSize() {
		return windowSize;
	}
	public void setWindowSize(int windowSize) {
		this.windowSize = windowSize;
	}
}
