package com.yeezhao.analyz.tweet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.yeezhao.analyz.tweet.TweetConsts.LabelType;
import com.yeezhao.commons.util.FreqDist;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;

/**
 * General tweet label model for ANY label type
 * 
 * @author congzicun/arber
 */
public class TwGeneralKwLabeller extends AbstractTwLabeller {

	private Map<String, LabelType> labelMap = new HashMap<String, LabelType>();
	private static final int maxKeywordLen = 8;
	private static final int minKeywordLen = 2;
	private final int ACT_AT_LEN = 2;

	public TwGeneralKwLabeller(LabelType label) {
		super(label);
	}

	@Override
	public double computeLabelWeight(TweetObj twObj) {
		List<Pair<Integer, Integer>> posPairList = StringUtil.backwardMaxMatch(
				twObj.getTwText().toUpperCase(), labelMap, maxKeywordLen,
				minKeywordLen);
		double weight = 0.0d;
		FreqDist<LabelType> frecDic = new FreqDist<LabelType>();
		for(Pair<Integer, Integer> posPair : posPairList){
			String word = twObj.getTwText().substring(posPair.first,posPair.second);
			frecDic.add(labelMap.get(word),	 1);
		}
		if (label.equals(LabelType.ACT)) {
			return computeACTWeight(twObj, frecDic.getCount(label));
		}

		else if (label.equals(LabelType.SRP)) {
			return computeSRPWeight(twObj, frecDic.getCount(label));
		}

		return weight;
	}

	private double computeSRPWeight(TweetObj twObj, int pairnum) {
		String wbWhTxt = twObj.getTwText().replaceAll(" ", "");

		// 只有表情的
		if (wbWhTxt.length() == 0) {
			return label.getWeight();
		}

		// 只有标点符号的
		if (wbWhTxt.replaceAll("\\.|,|，|。|？|〜|~|！|，|、|～|\\?", "").length() == 0) {
			return label.getWeight() * 10;
		}

		if (pairnum > 0)
			return label.getWeight();
		else
			return 0.0d;
	}

	private double computeACTWeight(TweetObj twObj, int pairnum) {
		String wbWhTxt = twObj.getTwText().replaceAll(" ", "");

		// 有微活动,并且还有@或者连接的
		if (pairnum > 0 && (twObj.isHasMotion() || twObj.isHasUrl())) {
			return label.getWeight() * 2;
		}

		if (pairnum > 0 && twObj.isRepost()) {
			return label.getWeight() * 2;
		}

		if (twObj.isHasMotion()) {
			// 只有@的
			if (wbWhTxt.length() < 1) {
				return label.getWeight();
			}
			// @数量大于3个的
			if (twObj.getMentions().size() >= ACT_AT_LEN) {
				return label.getWeight();
			}

			if (twObj.getTwText().matches("#[^#]+#")) {
				return label.getWeight();
			}
		}

		return 0.0d;
	}

	@Override
	public void setModel(String synonyms) {
		for (String s : synonyms.split(StringUtil.STR_DELIMIT_2ND))
			labelMap.put(s, label);
	}

}
