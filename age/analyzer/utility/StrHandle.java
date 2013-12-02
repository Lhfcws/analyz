package com.yeezhao.analyz.age.analyzer.utility;

import java.util.ArrayList;
import java.util.List;

/**
 * 字符串处理
 * @author user
 *
 */
public class StrHandle {
	private static List<String> list;
	static {
		list = new ArrayList<String>();
		list.add("\\t");
		list.add("\\n");
		list.add("\\r");
		list.add("\\f");
	}
	/**
	 * 去除转义字符
	 * @param text
	 * @return
	 */
	public static String removeEscape(String text) {
		String s = new String(text);
		for (String str : list) {
			s = s.replace(str, "");
		}
		return s;
	}
	/**
	 * 去除转义字符
	 * @param text
	 * @return
	 */
	public static String removeEscape2(String text) {
		String s = new String(text);	
		return s.replaceAll("[ \t\r\n\f]", "");
	}
	/**
	 * 去除转义字符
	 * @param text
	 * @return
	 */
	public static String removeEscape3(String text) {
		String s = new String(text);	
		return s.replaceAll("[\t\r\n\f]", "");
	}
	/**
	 * 去除html
	 * @param text
	 * @return
	 */
	public static String removeHmtl(String text) {
		String s = new String(text);	
		return s.replaceAll("<.*>", "");
	}
	/**
	 * 若原内容为空或null，返回长度为0的字符串
	 * @param text
	 * @return
	 */
	public static String noNullString(String text) {
		return text !=null? (text.equals("null") ? "" : text) : "";
	}
	/**
	 * 去除字符串中@后面的用户名，但保留@符号+空格。判断的用户名的截止条件是（空格/:/：）
	 * @param text
	 * @return
	 */
	public static String removeAt(String text) {
		char[] textChar = text.toCharArray();
		String str = "";
		for (int i = 0; i < textChar.length; i++) {
			if (textChar[i] == '@') {
				while (++i < textChar.length && (textChar[i] != ' ' && textChar[i] != ':'&& textChar[i] != '：')) {	
				}
				str += "@";
			} else {
				str += String.valueOf(textChar[i]);
			}
		}
		return str;
	}
}
