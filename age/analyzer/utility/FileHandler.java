package com.yeezhao.analyz.age.analyzer.utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class FileHandler {

	private final static Logger logger = Logger.getLogger(FileHandler.class);
	
	/**
	 * 读入文件到字符串列表
	 * @param inPutFileName
	 * @return
	 * @throws IOException
	 */
	public static List<String> readFileToList(InputStream stream) {
		List<String> stringList = new ArrayList<String>();
	//	File dirFile = new File(inPutFileName);
		if(stream == null){
			logger.error("stream not found: ");
			return stringList;
		}		
		try {
			BufferedReader input = new BufferedReader(new InputStreamReader(
					stream, "utf-8"));
		//	new FileInputStream(inPutFileName), "iso8859-1"));
		//	new FileInputStream(inPutFileName), "GB2312"));
		//	new FileInputStream(inPutFileName), "unicode"));
			
			while (true) {
				String line = input.readLine();
				if (line == null)
					break;
		//		line = removeBOMofUTF8(line);	// 去除UTF8文件的BOM头
				stringList.add(line);
			}
			input.close();

		} catch (Exception e) {
			logger.error("Read file error! " + e.getMessage());
			e.printStackTrace();
		}
		return stringList;
	}

	/**
	 * 读入文件到字符串列表
	 * @param inPutFileName
	 * @return
	 * @throws IOException
	 */
	public static List<String> readFileToList(String inPutFileName) {
		List<String> stringList = new ArrayList<String>();
		File dirFile = new File(inPutFileName);
		if(!dirFile.exists()){
			logger.error("file not found: " + inPutFileName);
			return stringList;
		}		
		try {
			BufferedReader input = new BufferedReader(new InputStreamReader(
					new FileInputStream(inPutFileName), "utf-8"));
		//	new FileInputStream(inPutFileName), "iso8859-1"));
		//	new FileInputStream(inPutFileName), "GB2312"));
		//	new FileInputStream(inPutFileName), "unicode"));
			
			while (true) {
				String line = input.readLine();
				if (line == null)
					break;
		//		line = removeBOMofUTF8(line);	// 去除UTF8文件的BOM头
				stringList.add(line);
			}
			input.close();

		} catch (Exception e) {
			logger.error("Read file error! " + e.getMessage());
			e.printStackTrace();
		}
		return stringList;
	}

	/**
	 * 将字符串列表写入到文件里
	 * @param stringList
	 * @param outPutFileName
	 * @throws IOException
	 */
	public static void writeListToFile(List<String> stringList, String outPutFileName) {
		try {
			BufferedWriter output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outPutFileName), "utf-8"));
			for (String string : stringList) {
				output.write(string);
				output.write("\n");
			}
			output.close();
		}catch (IOException e) {
			logger.error("Read file error! " + e.getMessage());
			e.printStackTrace();
		}
	}
	/**
	 * 去除UTF-8的BOM文件头
	 * @param text
	 * @return
	 */
	public static String removeBOMofUTF8(String text) {
		String str = "";
		char[] c = text.toCharArray();
		for (int i = 0; i < c.length; i++) {
			if (c[i] == (char)65279) {	// BOM头
				continue;
			}
			str += String.valueOf(c[i]);
		}
		return str;
	}
}
