package com.yeezhao.analyz.group;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WeiboContentCleaner implements DataCleanable {

	/**
	 * 过滤掉@和#引起的对象、话题名称，防止歧义
	 */
	public String cleanFootPrint(String weiboContent) {
		String expr = "(@[^#\\s.,\"\\?!:。，“”？！：']*)|(#[^#@]+#)";
		Pattern pattern = Pattern.compile(expr);
		Matcher matcher = pattern.matcher(weiboContent);
		StringBuilder sb = new StringBuilder();
		int ep = 0;
		while(matcher.find()){
			sb.append(weiboContent.substring(ep, matcher.start()));
			ep = matcher.end();
		}
		if(ep != weiboContent.length())
			sb.append(weiboContent.substring(ep));
		return sb.toString();
	}
	
}
