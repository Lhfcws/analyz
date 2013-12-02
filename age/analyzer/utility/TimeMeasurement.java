package com.yeezhao.analyz.age.analyzer.utility;

import java.text.SimpleDateFormat;
import java.util.Date;


public class TimeMeasurement {

	private long startTime = System.currentTimeMillis();  
	private long endTime = System.currentTimeMillis(); 
	
	public TimeMeasurement() {
		start();
	}
	public void printNowTime() {
		System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
	}
	public void start() {
		endTime = System.currentTimeMillis();
		startTime = System.currentTimeMillis();
	}
	public void stop() {
		endTime = System.currentTimeMillis();
	}
	public void pause() {
		endTime = System.currentTimeMillis();
	}
	public void reset() {
		endTime = System.currentTimeMillis();
		startTime = System.currentTimeMillis();
	}
	public String getTime() {
		return time2Str(endTime - startTime);
	}	
	public void printTime(String msg) {
	//	endTime = System.currentTimeMillis();
		System.out.println(msg + " " + time2Str(endTime - startTime));
	//	startTime = System.currentTimeMillis();
	}
	/**
	 * 将时间转换为字符串格式:分,秒,毫秒
	 * 
	 * @param ltime
	 * @return
	 */
	public static String time2Str(long ltime) {
		if (ltime < 1000) {
			return ltime + " ms"; // 以毫秒为单位
		} else if (ltime < 1000 * 60)// 以秒为单位
		{
			return (double) ltime / 1000 + " s";
		} else // 以分钟为单位
		{
			long m = ltime / (1000 * 60); // 分
			double s = ((double) (ltime % (1000 * 60)) / 1000); // 秒
			return m + " m " + s + " s";
		}
	}
}
