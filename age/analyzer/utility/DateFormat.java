package com.yeezhao.analyz.age.analyzer.utility;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateFormat {

	public static String date2DayString(Date date) {
		SimpleDateFormat sdt = new SimpleDateFormat("yyyy-M-d");
		return sdt.format(date);
	}
	public static String date2TimeString(Date date) {
		SimpleDateFormat sdt = new SimpleDateFormat("yyyy-M-d HH:mm:ss");
		return sdt.format(date);
	}
	public static Date dayString2Date(String dateString) {
		try {
			SimpleDateFormat sdt = new SimpleDateFormat("yyyy-M-d");
			Date date = new Date(sdt.parse(dateString).getTime());
			return date;			
		} catch (Exception e) {
			System.err.println(e);
		}
		return null;
	}
	public static Date timeString2Date(String dateString) {
		try {
			SimpleDateFormat sdt = new SimpleDateFormat("yyyy-M-d HH:mm:ss");
			Date date = new Date(sdt.parse(dateString).getTime());
			return date;			
		} catch (Exception e) {
			System.err.println(e);
		}
		return null;
	}
	/**
	 * 毫秒转化为字符串
	 * @param milliSecond
	 * @return
	 */
	public static String milliSecond2Sring(long milliSecond) {
		long second = milliSecond / 1000;
		String timeString = new String();
		
		timeString = second % 60 + " s ";	// 秒
		second /= 60;
		if (second > 0) {	// 分
			timeString = second % 60 + " m " + timeString;
			second /= 60;
		}
		if (second > 0) {	// 时
			timeString = second % 24 + " h " + timeString;
			second /= 24;
		}
		if (second > 0) {	// 天
			timeString = second + " d " + timeString;
		}
		return timeString;
	}
}
