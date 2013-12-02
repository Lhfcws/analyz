package com.yeezhao.analyz.tools;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.yeezhao.analyz.tweet.AbstractTwLabeller;
import com.yeezhao.analyz.tweet.TweetConsts.LabelType;
import com.yeezhao.analyz.tweet.TweetObj;
import com.yeezhao.commons.util.StringUtil;

public class TwFilterLabeller extends AbstractTwLabeller {
	private Set<String> emoticons = new HashSet<String>();
	private List<String> keywords = new LinkedList<String>();
	private Set<String> notOthers = new HashSet<String>();

	public TwFilterLabeller(LabelType labelType) {
		super(labelType);
	}

	@Override
	public double computeLabelWeight(TweetObj twObj) {
		double labelWeight = -1;
		labelWeight = processEmo(twObj);
		labelWeight += processKeyword(twObj.getTwText());
		// if (StringUtils.isEmpty(twObj.getTwText()))
		// return 1;
		if (twObj.getTwText().contains("?"))
			labelWeight = -1;
		return labelWeight;
	}

	private double processKeyword(String twString) {
		int num = 0;
		System.out.println("***"+twString);
		for (String kw : keywords) {
			if (twString.contains(kw))
				num++;
		}
		for (String kw : notOthers) {
			if (twString.contains(kw))
				num = -1;
		}
		if (ifStopWithPosWord(twString) || filterWholeText(twString))
			return 1;
		return num;
	}

	private double processEmo(TweetObj twObj) {
		int num = 0;
		for (String emoticon : twObj.getEmotion()) {
			if (emoticons.contains(emoticon))
				num++;
		}
		return num;
	}

	private boolean ifStopWithPosWord(String wbText) {
		if (wbText.trim().length() == 0)
			return false;
		System.out.println(wbText);
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
				|| wbText.indexOf("…") != -1 || wbText.indexOf("··") != -1
				|| wbText.indexOf("<") != -1 || wbText.indexOf("[") != -1)
			return true;
		return false;
	}

	@Override
	public void setModel(String mdlString) {
		String[] mdlElements = mdlString.split(":");
		if (mdlElements[0].equals("关键词"))
			keywords.addAll(Arrays.asList(mdlElements[1]
					.split(StringUtil.STR_DELIMIT_2ND)));
		else if (mdlElements[0].equals("表情")) {
			emoticons.addAll(Arrays.asList(mdlElements[1]
					.split(StringUtil.STR_DELIMIT_2ND)));
		} else if (mdlElements[0].equals("非其他"))
			notOthers.addAll(Arrays.asList(mdlElements[1]
					.split(StringUtil.STR_DELIMIT_2ND)));
	}

}
