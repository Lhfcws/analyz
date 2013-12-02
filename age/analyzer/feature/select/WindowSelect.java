package com.yeezhao.analyz.age.analyzer.feature.select;

import java.util.ArrayList;
import java.util.List;

import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;
import com.yeezhao.analyz.age.analyzer.weibo.Participle;

/**
 * 特征选择:窗口方法
 * a.搜索词周围的n个词、词性、以及kw-type （n待确定）
 * （给每个词性编号，取值范围为[0,1]，按词性出现在字典的顺序编序号并除以词性总个数）（目前n取4）
 * @author user
 *
 */
public class WindowSelect extends AbstractFeatureSelect {

	private int windowSize;	// 窗口大小
	
	public WindowSelect(List<Participle> keywordList, List<Participle> sFeatureList, int windowSize) {
		super(keywordList, sFeatureList);
		this.windowSize = windowSize;
		this.selectRepresentWord = "win";
	}

	@Override
	public List<Participle> select() {
		List<Participle> selectList = new ArrayList<Participle>();
		
		// 关键词下标位置
		List<List<Integer>> keywordIndexList = new ArrayList<List<Integer>>();
		for (Participle sf : keywordList) {
			keywordIndexList.add(getKeywordIndex(sf.getWord()));
		}
		
		for (List<Integer> kIndexs : keywordIndexList) {
			for (Integer i : kIndexs) {
				selectList.addAll(posTagSelect(textPartList, i, windowSize));	// 词性标注
				selectList.addAll(kwTypeSelect(textPartList, i, windowSize));	// kwType
				
			}
		}
		return selectList;
	}
	
	/**
	 * 词性选择
	 * @param list
	 * @param size
	 * @return
	 */
	private List<Participle> posTagSelect(List<Participle> list, int curIndex, int size) {
		List<Participle> selectList = new ArrayList<Participle>();
		selectList.addAll(posTagSelect(list, curIndex, size, true));
		selectList.addAll(posTagSelect(list, curIndex, size, false));
		return selectList;
	}

	private List<Participle> posTagSelect(List<Participle> list, int curIndex, int size, boolean isLeft) {
		List<Participle> subList = getSubList(list, curIndex, size, isLeft);
		List<Participle> selectList = new ArrayList<Participle>();
		String title = "postag_" + (isLeft ? "left" : "right");
		for (int i = 0; i < subList.size(); i++) {
			int index = isLeft ? (subList.size() - i) : i;
			String text = getRepresentWord(title + index) + "_" + subList.get(i).getPosTag();
			Participle sf = subList.get(i).partClone();
			sf.setWord(text);
			sf.setfAssignmentType(AssignmentType.TFIDF);
			selectList.add(sf);			
		}
		return selectList;
	}
	/**
	 * 关键词类型选择
	 * @param list
	 * @param size
	 * @return
	 */
	private List<Participle> kwTypeSelect(List<Participle> list, int curIndex, int size) {
		List<Participle> selectList = new ArrayList<Participle>();
		selectList.addAll(kwTypeSelect(list, curIndex, size, true));
		selectList.addAll(kwTypeSelect(list, curIndex, size, false));
		return selectList;
	}

	private List<Participle> kwTypeSelect(List<Participle> list, int curIndex, int size, boolean isLeft) {
		List<Participle> subList = getSubList(list, curIndex, size, isLeft);
		List<Participle> selectList = new ArrayList<Participle>();
		String title = "kwtype_" + (isLeft ? "left" : "right");
		for (int i = 0; i < subList.size(); i++) {
			int index = isLeft ? (subList.size() - i) : i;
			String text = getRepresentWord(title + index) + "_" + list.get(i).getKwTpye();
			Participle sf = list.get(i).partClone();
			sf.setWord(text);
			sf.setfAssignmentType(AssignmentType.TFIDF);
			selectList.add(sf);			
		}
		return selectList;
	}

	public int getWindowSize() {
		return windowSize;
	}
	public void setWindowSize(int windowSize) {
		this.windowSize = windowSize;
	}
}
