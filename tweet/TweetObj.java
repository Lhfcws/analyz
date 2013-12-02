package com.yeezhao.analyz.tweet;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 微博对象，注释掉了部分代码是为了做简单filter使用，在使用规则模型的时候要将注释去点
 * 
 * @author congzicun
 * 
 */
public class TweetObj {
	private String recvNickname; // corp nickname
	private String twText; // weibo text after clean
	private String[] subSentences; // 分句后的子句
	private List<String> mentions = new LinkedList<String>(); // @的nickname
	private String url; // urls in weibo
	private boolean isRepost = false; // if the weibo is repost from others
	private List<String> emotions = new LinkedList<String>(); // emotions in
																// weibo
	private boolean isResponse = false; // if the weibo is reponse to someone
										// else
	private boolean isHasMotion = false;
	private boolean isHasUrl = false;
	private String originWbText = null;

	/**
	 * @param twText
	 *            tweet text
	 * @param recvNickname
	 *            tweet reciever's nickname
	 */
	public TweetObj(String twText, String recvNickname) {
		this.recvNickname = recvNickname;
		originWbText = twText;
		twText = formatPunctruation(twText);
		twText = rmNoiseMsg(twText.toUpperCase());
		twText = rmRepost(twText);
		twText = rmResponse(twText);
		twText = extractAt(twText, recvNickname);
		twText = extractUrl(twText);
		twText = extractExpression(twText);
		twText = twText.replaceAll("[ ]+", " ");
		parseWeiboTxt(twText);
		this.twText = twText;
	}

	/**
	 * 丢弃非回复指定用户的所有信息
	 * 
	 * @param twText
	 * @return
	 */
	private String rmResponse(String twText) {
		Pattern pattern = Pattern.compile("回复[ |\t]*@[ |\t]*([^:]+)[ |\t]*:.*");
		Matcher matcher = pattern.matcher(twText);
		if (matcher.find()) {
			isResponse = true;
			if (!recvNickname.equals(matcher.group(1)))
				twText = matcher.replaceAll("");
		}
		return twText;
	}

	/**
	 * 将中文的符号转换为英文的
	 * 
	 * @param twText
	 * @return
	 */
	private String formatPunctruation(String twText) {
		return twText.replaceAll("，", ",").replaceAll("。", ".")
				.replaceAll("？", "?").replaceAll("！", "!").replaceAll("＠", "@")
				.replaceAll("～", "~").replaceAll("：", ":").replaceAll("；", ";")
				.replaceAll("『", "「").replaceAll("－", "-").replaceAll("（", "(")
				.replaceAll("）", ")").replaceAll("＿", "_")
				.replaceAll("“", "\"").replaceAll("“", "\"")
				.replaceAll("〈", "<").replaceAll("〉", ">").replaceAll("、", ",")
				.replaceAll("\\.\\.*", "…").replaceAll(",,,*", "…")
				.replaceAll("【", "[").replaceAll("】", "]");
	}

	/**
	 * 去掉使用客户端发送微博的时候出现的地理信息
	 */
	private String rmNoiseMsg(String twText) {
		return twText.replaceAll("我在[ |\t]*:[ |\t]*http[^ ]+", "");
	}

	/**
	 * 去掉转发的信息
	 * 
	 * @param twText
	 * @return
	 */
	private String rmRepost(String twText) {
		Pattern pattern = Pattern.compile("//[ |\t]*@.*");
		Matcher matcher = pattern.matcher(twText);
		if (matcher.find()) {
			twText = matcher.replaceAll("");
			isRepost = true;
		}
		return twText.trim();
	}

	/**
	 * 提取微博中的url
	 * 
	 * @param doc
	 * @param wbTxt
	 * @return
	 */
	private String extractUrl(String wbTxt) {
		Pattern pattern = Pattern.compile("HTTP:[^ ]* *");
		Matcher matcher = pattern.matcher(wbTxt);
		boolean first = true;
		StringBuilder sb = new StringBuilder();
		while (matcher.find()) {
			if (!first)
				sb.append("|");
			sb.append(matcher.group());
			first = false;
			isHasUrl = true;
		}
		if (first == false)
			url = sb.toString();

		return matcher.replaceAll("").trim();
	}

	/**
	 * 提取微博中的@信息.如果没有@corp，那么就认为一条是可以丢弃的
	 * 
	 * @param doc
	 * @param wbTxt
	 */
	private String extractAt(String twText, String recvNickname) {
		Pattern pattern = Pattern.compile("@([^ |^@|^:]+) *");
		Matcher matcher = pattern.matcher(twText);
		boolean mentionCorp = false;
		while (matcher.find()) {
			String usrNick = matcher.group(1);
			if (usrNick.equals(recvNickname))
				mentionCorp = true;
			mentions.add(usrNick);

		}
		if (mentionCorp == false && mentions.size() != 0)
			return "";
		return matcher.replaceAll("");

	}

	/**
	 * 将微博中的表情提取出来,保存在emotions中
	 * 
	 * @param doc
	 * @param wbTxt
	 * @return
	 */
	private String extractExpression(String twText) {
		Pattern pattern = Pattern.compile("\\[[^\\]]+\\]");
		Matcher matcher = pattern.matcher(twText);
		while (matcher.find()) {
			emotions.add(matcher.group());
		}
		return matcher.replaceAll("").trim();
	}

	/**
	 * 对微博按照标点分句，每个分句会保留自己的自己后面所跟随的符号
	 * 
	 * @param wbTxt
	 * @return
	 */
	private void parseWeiboTxt(String wbTxt) {
		Pattern pattern = Pattern.compile("(\\?|!|\\.|,|…|=| )+");
		Matcher matcher = pattern.matcher(wbTxt);
		String[] wbTxts = wbTxt.split("(\\?|!|\\.|,|…|=| )+");
		if (wbTxts.length > 0) {
			int count = 0;
			while (count < wbTxts.length) {
				if (matcher.find()) {
					wbTxts[count] += matcher.group();
				}
				count++;
			}
		}
		if (wbTxts.length == 0)
			wbTxts = new String[] { wbTxt };

		subSentences = wbTxts;
	}

	public String getRecvNickname() {
		return recvNickname;
	}

	public String getTwText() {
		return twText;
	}

	public String[] getSubSentences() {
		return subSentences;
	}

	public List<String> getMentions() {
		return mentions;
	}

	public String getUrl() {
		return url;
	}

	public boolean isRepost() {
		return isRepost;
	}

	public List<String> getEmotion() {
		return emotions;
	}

	public boolean isResponse() {
		return isResponse;
	}

	public boolean isHasMotion() {
		return isHasMotion;
	}

	public boolean isHasUrl() {
		return isHasUrl;
	}

	public String getOriTwText() {
		return originWbText;
	}
}
