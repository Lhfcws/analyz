package com.yeezhao.analyz.tweet;


import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.yeezhao.analyz.tweet.TweetConsts.LabelType;
import com.yeezhao.analyz.tweet.TweetConsts.WordType;
import com.yeezhao.commons.util.FreqDist;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;

/**
 * A question label model
 * 
 * @author congzicun/arber
 */
public class TwQuestionLabeller extends AbstractTwLabeller {

	private static final int maxKeywordLen = 8;
	private static final int minKeywordLen = 1;

	private Map<String, List<WordType>> dict = new HashMap<String, List<WordType>>(); //<词, 词类型>
	
	public TwQuestionLabeller() {
		super(LabelType.QST);
	}

	@Override
	public double computeLabelWeight(TweetObj twObj) {
		double totalWeight = 0.0d;
		int subSntns = twObj.getSubSentences().length;
		for( int pos = 0; pos < subSntns; pos++ ){
			String wbTxt = twObj.getSubSentences()[pos].toUpperCase();

			List<Pair<Integer, Integer>> posPairList = StringUtil
					.backwardMaxMatch(wbTxt, dict, maxKeywordLen, minKeywordLen);
			FreqDist<WordType> wordDist = new FreqDist<WordType>();
			for( Pair<Integer, Integer> pair: posPairList ){
				String word = wbTxt.substring(pair.first, pair.second);
				wordDist.addAll(dict.get(word));
			}

			int qstWordNum = wordDist.getCount(WordType.QST);
			int objWordNum = wordDist.getCount(WordType.OBJ);
			int noiseWordNum = wordDist.getCount(WordType.NOISE); 
			int toneWordNum = wordDist.getCount(WordType.TONE);
			int combWordNum = wordDist.getCount(WordType.COMB);
			
			//语气词不能与表示非疑问的标点符号相连
			Pattern pattern = Pattern.compile(",|!|\\.|，|。|…");
			Matcher mtch = pattern.matcher(wbTxt);
			if(mtch.find()){
				toneWordNum = 0;
			}
			
			//max( min(组合词, 对象), min(组合词, 语气词) + 疑问词 )
			int pairnum = Math.max(
					Math.min(combWordNum, objWordNum),
					Math.min(combWordNum, toneWordNum))
					+ qstWordNum;
			
			totalWeight += computeQstWeight(wbTxt, pairnum, noiseWordNum, pos,
						subSntns);
		}
		
		return totalWeight;
	}
	/**
	 * 
	 * @param doc
	 * @param pairnum	上一步计算出来的parinum
	 * @param noisenum	包含“你，你们”等对象指向性词的数目
	 * @param pos		分句在整个微博中的位置
	 * @param numOfSent	整个微博的中分句的数量
	 * @return
	 */
	private double computeQstWeight(String wbTxt, int pairnum, int noisenum, int pos,
			int numOfSent) {
		//如果语句的长度小于2,不认为是咨询
		if(wbTxt.replaceAll("\\.|,|，|。|？|〜|~|！|，|、|～|\\?", "").length() <2)
			return 0.0d;

		double weight = pairnum * label.getWeight();
		
		int qstnum = wbTxt.length() * 2 - wbTxt.replaceAll("\\?", "").length()
				- wbTxt.replaceAll("？", "").length();

		int othernum = wbTxt.length() * 5 - wbTxt.replaceAll("!", "").length()
				- wbTxt.replaceAll("！", "").length()
				- wbTxt.replaceAll("。。。", "").length()
				- wbTxt.replaceAll("\\.\\.\\.", "").length()
				- wbTxt.replaceAll("……", "").length();

		weight = weight - (double) othernum / 2;
		//问号只有出现在首部和尾部的时候才会对咨询有贡献
		if ( (pos == 0 || pos == numOfSent-1) && qstnum != 0 && noisenum == 0)
			weight += label.getWeight();
		
		return weight;
	}


	public void setModel(String synonyms) {
		String[] strAry = synonyms.split(":");
		WordType wordType = WordType.fromType( strAry[0] );
		if( wordType == null ) 
			return;
		
		String[] wordAry = strAry[1].split(StringUtil.STR_DELIMIT_2ND);
		for (String s : wordAry){
			List<WordType> wTypeList = dict.containsKey(s)?
					dict.get(s):(new LinkedList<WordType>());
			wTypeList.add(wordType);
			dict.put(s, wTypeList);
		}
	}
}
