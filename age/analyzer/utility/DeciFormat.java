package com.yeezhao.analyz.age.analyzer.utility;

import java.text.DecimalFormat;

public class DeciFormat {

	private static DecimalFormat df1 = new DecimalFormat("#.000"); // 格式化
	private static DecimalFormat df2 = new DecimalFormat("#.0"); // 格式化
	private static DecimalFormat df3 = new DecimalFormat("#,###,###,###"); // 格式化

	/**
	 * 保留三位小数
	 * @param value
	 * @return
	 */
	public static String percent3(double value) {		
		return df1.format(value * 100) + "%";
	}
	/**
	 * 保留一位小数
	 * @param value
	 * @return
	 */
	public static String percent1( double value) {		
		return df2.format(value * 100) + "%";
	}

	public static String percent1(String title, double value) {
		return title + percent1(value);
	}	
	public static String percent3(String title, double value) {
		return title + percent3(value);
	}	
	
	public static void printPercent1(String title, double value) {
		System.out.println(percent1(value));
	}
	public static void printPercent3(String title, double value) {
		System.out.println(percent3(value));
	}

	public static String comma(int value) {		
		return df3.format(value);
	}
	public static String format1( double value) {		
		return df2.format(value);
	}
	public static String format3( double value) {		
		return df1.format(value);
	}
	public static String format1or3( double value) {
		return value > 1 ? format1(value) : format3(value);
	}
}
