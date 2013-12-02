package com.yeezhao.analyz.age.analyzer;

import java.io.File;
import java.util.Calendar;
import java.util.Locale;

import org.apache.log4j.Logger;

import com.yeezhao.analyz.age.analyzer.utility.Common;



/**
 * 输出文件处理类
 * 将所有的抓取结果写到本地文件中
 * @author weichaoo
 *
 */
public class OutFileHandler {
	
	private final Logger logger = Logger.getLogger(OutFileHandler.class);

	public static final String SINA_PATH = System.getProperty("user.dir") + File.separatorChar + "analysis_output" + File.separatorChar + "sina" + File.separatorChar;
	public static final String TX_PATH = System.getProperty("user.dir") + File.separatorChar + "analysis_output" + File.separatorChar + "tencent" + File.separatorChar;


	public void saveMsg2FileByday(String text) {
		String path = SINA_PATH;
		if(createDirFile(path)){
			Calendar cal = Calendar.getInstance(Locale.CHINA);
			String dirFile = path + getDirname(cal);
			if(createDirFile(dirFile)){
				String filename = dirFile + File.separatorChar + getFilenameByDay(cal) + ".txt";
				saveShopProductIdToFile(text, filename);
			}
		}
	}
	public void saveMsg2File(String text) {
		String path = SINA_PATH;
		if(createDirFile(path)){
			Calendar cal = Calendar.getInstance(Locale.CHINA);
			String dirFile = path + getDirname(cal);
			if(createDirFile(dirFile)){
				String filename = dirFile + File.separatorChar + getFilename(cal) + ".txt";
				saveShopProductIdToFile(text, filename);
			}
		}
	}
	private boolean createDirFile(String dirname){
		File dirFile = new File(dirname);
		if(!dirFile.exists()){
			boolean flag = dirFile.mkdir();
			if(flag){
				return true;
			}
			else {
				logger.error("Create File Directory Faile: " + dirname);
				return false;
			}
		}
		return true;
	}
	private String getFilename(Calendar cal){
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		String filename = getDirname(cal) + "-" + String.valueOf(hour);
		return filename;
	}
	private String getFilenameByDay(Calendar cal){
		String filename = getDirname(cal);
		return filename;
	}
	private  String getDirname(Calendar cal){
		int year = cal.get(Calendar.YEAR);
		int month = cal.get(Calendar.MONTH) + 1;
		int day = cal.get(Calendar.DAY_OF_MONTH);
		String dirname = String.valueOf(year)+"-"+String.valueOf(month)+"-"+String.valueOf(day);
		return dirname;
	}
	/**
	 * 将抓取的结果保存到文件中
	 */
	private void saveShopProductIdToFile(String text, String filename) {
		Common.appendFile(filename, text + "\n");
	}
}
