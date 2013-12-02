package com.yeezhao.analyz.filter;
/**
 * 对微博文本进行一些处理
 * @author congzicun
 */
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.yeezhao.commons.classifier.base.Document;
import com.yeezhao.commons.util.StringUtil;



public class WeiboProcess {
	/**
	 * 判断微博是否是转发自其他用户
	 * @param wbTxt
	 * @return
	 */
	public static boolean ifRepost(String wbTxt) {
		Pattern pattern = Pattern.compile("//[ ]*@.*");
		Matcher matcher = pattern.matcher(wbTxt);
		if (matcher.find()) {
			return true;
		}
		return false;
	}
	/**
	 * 去掉微博中其他用户的转发信息
	 * @param wbTxt
	 * @return
	 */
	public static String rmOthMsg(String wbTxt) {
		if (ifRepost(wbTxt)) 
			wbTxt = wbTxt.replaceAll("//[ ]*@.*", "");
		return wbTxt;
	}
	
	/**
	 * 分析微博文本中是否包含url
	 * @param content
	 * @return
	 */
	public static boolean statisticUrl(String content) {
		
		//去掉因为勋章或者我在引入的url
		Pattern pattern = Pattern.compile("勋章|我在");
		Matcher m = pattern.matcher(content);
		if (m.find())
			return false;
		pattern = Pattern.compile("http:[^ |^,|^，]*");
		m = pattern.matcher(content);
		if (m.find()) {
			return true;
		}
		return false;
	}
	
	/**
	 * 判断微博是否是纯转发
	 * @param info
	 * @param content
	 * @return
	 */
	public static boolean statisticRps(String content) {
		Pattern pattern = Pattern.compile("转发微博|轉發微博|Repost");
		Matcher matcher = pattern.matcher(content);
		if (matcher.find()) {
			return true;
		}
		return false;
	}
	/**
	 * 提取用户的@信息
	 * @param doc
	 * @param wbTxt
	 * @return
	 */
	public static String extractAt(Document doc,String wbTxt){
		wbTxt = wbTxt.replaceAll("回复@([^ |^@|^:]*)", "");
		Pattern pattern = Pattern.compile("@([^ |^@|^:]*)");
		Matcher matcher = pattern.matcher(wbTxt);
		StringBuilder sbat = new StringBuilder();
		boolean first = true;
		while (matcher.find()) {
			if (!first)
				sbat.append(StringUtil.DELIMIT_1ST);
			sbat.append(matcher.group(1));
			first = false;
		}
		if (first == false) {
			doc.putField(WeiboConsts.ATT_WB_AT, sbat.toString());
		}
		return matcher.replaceAll("");
	}
}
