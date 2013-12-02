package com.yeezhao.analyz.filter;

/**
 * congzicun/yongjiang
 *  modified 2012-09-09
 *  modified 2012-11-01
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.yeezhao.amkt.core.CoreConsts.USER_TYPE;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.commons.classifier.base.Classifier;
import com.yeezhao.commons.classifier.base.DocProcessor;
import com.yeezhao.commons.classifier.base.Document;

public class UserFilterOp implements Classifier, DocProcessor {
	private UNameFilter unameFilter;

	public UserFilterOp(Configuration conf) throws IOException {
		unameFilter = UNameFilter.initFilter(conf
				.getConfResourceAsInputStream(conf
						.get(AppConsts.FILE_UNAME_FILTER)));
	}

	@Override
	public Document process(Document doc) throws Exception {
		String result = classify(doc);
		if (result != null)
			doc.putField(AppConsts.COL_USR_TYPE, result);
		return doc;
	}

	@Override
	public String classify(Document doc) {
		USER_TYPE userType = USER_TYPE.NORMAL;
		if (doc.containsField(AppConsts.COL_USR_NICKNAME)
				&& unameFilter.filter(doc.get(AppConsts.COL_USR_NICKNAME)))
			userType = USER_TYPE.ORG;
		else if (isRobot(doc, null, null))
			userType = USER_TYPE.ROBOT;
		return Integer.toString(userType.getCode());
	}

	public String classify(Document doc, List<String> tweets, List<Long> time) {
		USER_TYPE userType = USER_TYPE.NORMAL;
		if (doc.containsField(AppConsts.COL_USR_NICKNAME)
				&& unameFilter.filter(doc.get(AppConsts.COL_USR_NICKNAME)))
			userType = USER_TYPE.ORG;
		else if (isRobot(doc, tweets, time))
			userType = USER_TYPE.ROBOT;

		return Integer.toString(userType.getCode());
	}
	
	private boolean isRobot(Document doc, List<String> tweets, List<Long> time) {
		// 关注数, 粉丝数，微博数 任意一个小于等于5，则认为是僵尸粉
		int friendNum = getIntVal(doc, AppConsts.COL_USR_FRIENDCOUNT);
		int followNum = getIntVal(doc, AppConsts.COL_USR_FOLLOWCOUNT);
		int statusCount = getIntVal(doc, AppConsts.COL_USR_STATUSCOUNT);
		int tweetCrawled = getIntVal(doc, AppConsts.COL_USR_TWEETNUM);

		if (friendNum > 0 && friendNum <= 5)
			return true;
		if (followNum > 0 && followNum <= 5)
			return true;
		if (statusCount > 0 && tweetCrawled > 0 && // Label those only we have
													// crawled data
				statusCount >= tweetCrawled && // Double confirm statusCount is
												// correct
				statusCount <= 5)
			return true;

		// 根据用户微博属性来进行判断
		boolean rst = false;
		if (tweets != null && classifyTweets(tweets))
			rst = true;
		else if (time != null && classifyTime(time))
			rst = true;
		return rst;
	}

	/**
	 * 通过统计微博的发送间隔来判断是否为僵尸
	 * 
	 * @param time
	 * @return
	 */
	private boolean classifyTime(List<Long> time) {
		Collections.sort(time);
		long pre = 0;
		int amount = time.size();

		ArrayList<Long> period = new ArrayList<Long>();
		for (long t : time) {
			if (pre != 0) {
				period.add(Math.abs(pre - t));
			}
			pre = t;
		}

		// 连续发送的微博的数量
		int continuous_wb = 0;
		// 符合条件的连续发送间隔的长度
		int len = 0;
		long ori = 0;
		for (int j = 0; j < period.size(); j++) {
			if (j == 0) {
				ori = period.get(j);
				continue;
			}
			// 连续的发送间隔之差可以在一分钟内浮动，但是不能比初始发送间隔大2分钟
			// 时间是经过测试得到的，如下的时间可以保证较高的准确率
			// 为了提高召回率可以适当的放宽时间间隔
			if (Math.abs(period.get(j) - period.get(j - 1)) < 60
					&& period.get(j) - ori < 120)
				len++;
			else {
				if (len > 1)
					continuous_wb += (len + 1);
				len = 0;
				ori = period.get(j);
			}
		}
		// 如果超过半数的微博都属于连续发送的状态，那么认为是僵尸
		if (continuous_wb * 2 > amount)
			return true;
		return false;
	}

	/**
	 * 如果用户的微博纯转发和url超过80%，就认为是僵尸
	 * 
	 * @param tweets
	 * @return
	 */
	private boolean classifyTweets(List<String> tweets) {
		int amount = tweets.size();
		int url = 0;
		int pureReopst = 0;
		for (String wbTxt : tweets) {
			wbTxt = WeiboProcess.rmOthMsg(wbTxt);
			if (WeiboProcess.statisticUrl(wbTxt))
				url++;
			else if (WeiboProcess.statisticRps(wbTxt)) {
				pureReopst++;
			}
		}
		if (url + pureReopst >= amount * 0.8)
			return true;
		else
			return false;
	}

	private int getIntVal(Document doc, String fieldName) {
		try {
			return doc.containsField(fieldName) ? Integer.parseInt(doc
					.get(fieldName)) : -1;
		} catch (NumberFormatException ex) {
			return -1;
		}
	}

}
