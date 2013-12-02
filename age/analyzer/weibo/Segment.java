package com.yeezhao.analyz.age.analyzer.weibo;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import jeasy.analysis.MMAnalyzer;

import org.apache.log4j.Logger;

public class Segment {

	private final static Logger logger = Logger.getLogger(Segment.class);
	
	private static ICTCLAS50 newICTCLAS50;	
	static{
		init();
	}
	public static void init() {
		final String CONF_PARH = System.getProperty("user.dir") + File.separatorChar + "conf" + File.separatorChar + "ICTCLAS50";
		final String userDir = CONF_PARH + File.separatorChar + "csmt_userdict.txt";		//用户自定义词典
		try {
			newICTCLAS50 = new ICTCLAS50();
			String argu = CONF_PARH;
			if (newICTCLAS50.ICTCLAS_Init(argu.getBytes("GB2312")) == false) {
				logger.error("ICTCLAS50 Init Fail!");
				return;
			}
			// 设置词性标注集(0 计算所二级标注集，1 计算所一级标注集，2 北大二级标注集，3 北大一级标注集)
			newICTCLAS50.ICTCLAS_SetPOSmap(2);	
			// 导入用户字典,可选。返回导入用户词语个数第一个参数为用户字典路径，第二个参数为用户字典的编码类型
			newICTCLAS50.ICTCLAS_ImportUserDictFile(userDir.getBytes(), 0);
		} catch (UnsupportedEncodingException e) {
			logger.error(e);
			e.printStackTrace();
		}
	}
	
	public static void main(String[] arg) {
		
		List<String> segmentedList = new ArrayList<String>();
		segmentedList.add("你妹哦，早上刷牙的时候是感觉没有清香味，原来把洗面奶挤到牙刷上了。狂吐中……");
	//	segmentedList.add("用洗面奶洗面，原来把洗面奶挤到牙刷上了。");
	//	segmentedList.add("今天洗面的时候遇上了奇怪的事情");
	//	segmentedList.add("哥哥开得不是挖机开得是寂寞，挖机转的是360度，哥穿的是361度。悲哀，的确悲哀");
		segmentedList.add("寒假想去新加坡啊！可惜看中的自由行产品没有了...发愁！	");
	//	segmentedList.add("@今天@洗:发 @的 @时 候@");
		segmentedList.add("这是注释 /");
	//	segmentedList.add(" ");
	//	segmentedList.add("随后温总理就离开了舟曲县城，预计温总理今天下午就回到北京。以上就是今天上午的最新动态。◎成交价由1.23万元/平米降至9087元/平米");
	//	segmentedList = Segment.ICTCLAS_ParagraphProcess(segmentedList); 	// 对正文件内容做分词

	/*	for (String string : Segment.ICTCLAS_ParagraphProcess(segmentedList)) {
			System.out.println(string);
		}*/
		
		for (String line : segmentedList) {
			String str = Segment.ICTCLAS_WordProcess(line, 0);
			System.out.println(str);
			System.out.println(Jeasy_WordProcess(line));
		}
		
		System.out.println("Segment.main() all done!");
	}

	/**
	 * Jeasy分词
	 * @param text
	 * @return
	 */
	public synchronized static String[] Jeasy_WordProcess(String text){ 	
    	MMAnalyzer analyzer = new MMAnalyzer(); 	
		try {
			String nativeStr = analyzer.segment(text, " ");
			return nativeStr.split(" ");
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("Jeasy err: ", e);
			return null;
		}
	}
	/**
	 * Jeasy为列表做分词
	 * @param sInput
	 * @return
	 */
	public synchronized static List<String> Jeasy_ParagraphProcess(List<String> sInputList) {
		List<String> segmentedListList = new ArrayList<String>();

		for (String sInput : sInputList) {
			MMAnalyzer analyzer = new MMAnalyzer();
			try {
				String nativeStr = analyzer.segment(sInput, " ");
				segmentedListList.add(nativeStr);
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("Jeasy err: ", e);
			}
		}
		return segmentedListList;
	}

	

	/**
	 * 单个词的分词
	 * @param strInput
	 * @return
	 */
	public synchronized static String[] getWordSegment(String strInput){
		try {			
			byte nativeBytes[] = newICTCLAS50.ICTCLAS_ParagraphProcess(strInput.getBytes("GB2312"), 2, 0);
			String nativeStr = new String(nativeBytes, 0,nativeBytes.length, "GB2312");
			String[] tests = nativeStr.split(" ");
			return tests;
		} catch (UnsupportedEncodingException e) {
			logger.error("error:UnsupportedEncodingException", e);
			System.err.println(e.getMessage());
			return null;
		}
	}
	
	/**
	 * 为列表做分词
	 * @param sInput
	 * @return
	 */
	public synchronized static List<String> ICTCLAS_ParagraphProcess(List<String> sInputList) {
		List<String> segmentedListList = new ArrayList<String>();
		for (String sInput : sInputList) {
			String nativeStr = ICTCLAS_WordProcess(sInput, 0);
			segmentedListList.add(nativeStr);
		}
		return segmentedListList;
	}

	/**
	 * 单个词分词
	 * @param strInput
	 * @param bPOSTagged 是否带词性，0=不带，1=带
	 * @return
	 */
	public synchronized static String ICTCLAS_WordProcess(String strInput, int bPOSTagged){
		try {
			byte nativeBytes[] = newICTCLAS50.ICTCLAS_ParagraphProcess(strInput.getBytes("GB2312"), 2, bPOSTagged);	// 是否带词性
			String nativeStr = new String(nativeBytes, 0, nativeBytes.length, "GB2312");
			return nativeStr;
		} catch (Exception ex) {
			// 释放分词组件资源
			newICTCLAS50.ICTCLAS_Exit();
			logger.error("error:UnsupportedEncodingException", ex);
			System.err.println(ex.getMessage());
			ex.printStackTrace();
			return null;
		}
	}
	/**
	 * 带词性标注的分词
	 * @param strInput
	 * @return
	 */
	public synchronized static String[][] ICTCLAS_WordProcessWithPos(String strInput){
		String nativeStr = ICTCLAS_WordProcess(strInput, 1);
		String[] split = nativeStr.split(" ");
		String[][] segmenteds = new String[split.length][2];
		for (int i = 0; i < split.length; i++) {
			String text = split[i];
			String[] posSplit = text.split("/");
			if (posSplit.length == 2) {
				segmenteds[i][0] = posSplit[0];
				segmenteds[i][1] = posSplit[1];
			} else {
				int lastIndex = text.lastIndexOf("/");
				if (lastIndex > -1) {
					segmenteds[i][0] = text.substring(0, lastIndex);
					segmenteds[i][1] = text.substring(lastIndex + 1, text.length());
				} else {
					segmenteds[i][0] = "";
					segmenteds[i][1] = "";
				}
			}
		}
		
		return segmenteds;
	}
}
