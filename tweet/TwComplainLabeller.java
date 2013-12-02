package com.yeezhao.analyz.tweet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yeezhao.analyz.tweet.TweetConsts.LabelType;
import com.yeezhao.analyz.tweet.TweetConsts.WordType;
import com.yeezhao.commons.util.FreqDist;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;

/**
 * A complaint label model: compute weight based the frequencies of different
 * word type.
 * 
 * 
 * 现在注释掉了部分代码，是为了做简单的filter使用
 * 
 * @author zicun/arber
 */
public class TwComplainLabeller extends AbstractTwLabeller {
	Log LOG = LogFactory.getLog(TwComplainLabeller.class);
	private static final int maxKeywordLen = 8;
	private static final int minKeywordLen = 1;

	private Map<String, List<WordType>> dict = new HashMap<String, List<WordType>>(); // <词,
																						// 词类型>
	private Set<String> noiseWords = new HashSet<String>();
	private Set<String> emotionWords = new HashSet<String>();

	public TwComplainLabeller() {
		super(LabelType.CMP);
	}

	@Override
	public double computeLabelWeight(TweetObj twObj) {
		double totalWeight = 0.0d;
		int subSntns = twObj.getSubSentences().length;
		LOG.info("text: " + twObj.getTwText());
		if (filterWholeText(twObj.getTwText()))
			return 0;

		if (filterEmotion(twObj.getEmotion()))
			return 0;

		for (int pos = 0; pos < subSntns; pos++) {

			String wbTxt = rmNoiseWords(twObj.getSubSentences()[pos]);
			if (ifStopWithPosWord(wbTxt))
				return 0;
			FreqDist<WordType> wordDist = processMatchRst(wbTxt);
			// min(修饰+对象)+情感词-正面词
			int pairnum = Math.min(wordDist.getCount(WordType.ADJ),
					wordDist.getCount(WordType.OBJ))
					+ wordDist.getCount(WordType.NEG)
					- wordDist.getCount(WordType.POS);
			LOG.info("pair num:" + pairnum);
			totalWeight += computeLocalCMPWeight(wbTxt, pairnum);
			LOG.info("total weight: " + totalWeight);
		}
		for (String emotionString : twObj.getEmotion()) {
			if (emotionString.contains("怒") || emotionString.contains("鄙视")
					|| emotionString.contains("吐"))
				totalWeight += label.getWeight();
		}
		LOG.info(totalWeight);
		return totalWeight;
	}

	private FreqDist<WordType> processMatchRst(String wbTxt) {
		List<Pair<Integer, Integer>> posPairList = StringUtil.backwardMaxMatch(
				wbTxt, dict, maxKeywordLen, minKeywordLen);
		FreqDist<WordType> wordDist = new FreqDist<WordType>();
		WordType previousType = null;
		int previousKWPos = 0;
		WordType currentWDType = null;
		for (Pair<Integer, Integer> pair : posPairList) {
			String word = wbTxt.substring(pair.first, pair.second);
			currentWDType = dict.get(word).get(0);
			LOG.info("matcher words: " + word + "pos:<" + pair.first + ","
					+ pair.second + "> wordType: " + dict.get(word).get(0));

			if (previousType == WordType.PRVTV) {
				if (currentWDType == WordType.POS
						&& previousKWPos + 1 == pair.second)
					wordDist.add(WordType.NEG, 1);
				else if (currentWDType == WordType.NEG
						&& previousKWPos + 1 == pair.second)
					wordDist.add(WordType.NEG, 1);
			} else
				wordDist.addAll(dict.get(word));

			previousType = currentWDType;
			previousKWPos = pair.second;
		}
		return wordDist;
	}

	private String rmNoiseWords(String wbTxt) {
		for (String word : noiseWords) {
			wbTxt = wbTxt.replaceAll(word, "");
		}
		return wbTxt;
	}

	private double computeLocalCMPWeight(String wbTxt, int pairnum) {
		double weight = pairnum * label.getWeight();
		// 如果在语句中包含～，投诉的权重降低
		// if (wbTxt.indexOf("..") != -1 || wbTxt.indexOf("…") != -1) {
		// weight -= label.getWeight();
		// LOG.info("matched punctruation");
		// }
		return weight;
	}

	private boolean filterEmotion(List<String> emotions) {
		for (String emotion : emotions) {
			for (String emow : emotionWords) {
				if (emotion.contains(emow))
					return true;
			}
		}
		return false;
	}

	private boolean ifStopWithPosWord(String wbText) {
		if (wbText.trim().length() == 0)
			return false;
		int lastPosition = wbText.length() - 1;
		if (wbText.indexOf("滴") == lastPosition || wbText.indexOf("哦") != -1
				|| wbText.indexOf("啦") != -1 || wbText.indexOf("哈") != -1
				|| wbText.indexOf("呀") != -1
				|| wbText.indexOf("丫") == lastPosition
				|| wbText.indexOf("吖") != -1 || wbText.indexOf("哇") != -1
				|| wbText.indexOf("噢") != -1
				|| wbText.indexOf("亲") == lastPosition
				|| wbText.indexOf("咯") != -1 || wbText.indexOf("呐") != -1
				|| wbText.indexOf("呗") != -1)
			return true;
		return false;
	}

	private boolean filterWholeText(String wbText) {
		if (wbText.indexOf(":") != -1 || wbText.indexOf("【") != -1
				|| wbText.indexOf("《") != -1 || wbText.indexOf("「") != -1
				|| wbText.indexOf("〕") != -1 || wbText.indexOf("——") != -1
				|| wbText.indexOf("-") != -1 || wbText.indexOf(";") != -1
				|| wbText.indexOf("~") != -1 || wbText.indexOf("=") != -1
				|| wbText.indexOf("…") != -1 || wbText.indexOf("··") != -1)
			return true;
		return false;
	}

	public void setModel(String synonyms) {
		String[] strAry = synonyms.split(":");
		WordType wordType = WordType.fromType(strAry[0]);
		if (wordType == null)
			return;

		String[] wordAry = strAry[1].split(StringUtil.STR_DELIMIT_2ND);
		if (wordType.equals(WordType.NOISE)) {
			noiseWords.addAll(Arrays.asList(wordAry));
		} else if (wordType.equals(WordType.EMOTN)) {
			emotionWords.addAll(Arrays.asList(wordAry));
		} else {
			for (String s : wordAry) {
				List<WordType> wTypeList = dict.containsKey(s) ? dict.get(s)
						: (new LinkedList<WordType>());
				wTypeList.add(wordType);
				dict.put(s, wTypeList);

			}
		}
	}
}
