package com.yeezhao.analyz.age.analyzer.weibo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.yeezhao.analyz.age.analyzer.utility.FileHandler;
import com.yeezhao.analyz.age.analyzer.utility.StrHandle;


public class WeiboMenagment {

	public static List<Weibo> addFeature(List<Weibo> weiboList) {
		for (Weibo weibo : weiboList) {
			List<Participle> textList = new ArrayList<Participle>();
			String[][] posText = Segment.ICTCLAS_WordProcessWithPos(weibo.getContent());
			for (int i = 0; i < posText.length; i++) {
				Participle sf = new Participle(posText[i][0]);
				sf.setPosTag(posText[i][1]);	// 设置词性
				textList.add(sf);
			}
			weibo.addTextPartList(textList);
		}
		return weiboList;
	}
	public static List<Weibo> addExpFeature(List<Weibo> weiboList) {
/*		for (Weibo weibo : weiboList) {
			weibo.addTextPartList(getExpFeature(weibo));
		}*/
		return weiboList;
	}
	
	/**
	 * 增加额外的特征
	 * @param weibo
	 * @return
	 */
	public static List<Participle> getExpFeature(Weibo weibo) {
		List<Participle> expList = new ArrayList<Participle>();
		
		List<Participle> keywordList = WeiboMenagment.getKeyWordList(weibo);
		List<Participle> textList = WeiboMenagment.getTextList(weibo);
			

	//	expList.addAll(new LabelledSelect(keywordList, textList, weibo.getLabel()).select());		// a.窗口		
		
/*		expList.addAll(new WindowSelect(keywordList, textList, 4).select());		// a.窗口		
		expList.addAll(new KeywordLengthSelect(keywordList, textList).select());	// b.关键词长度
		expList.addAll(new KeywordDistSelect(keywordList, textList).select());		// c.关键词距离
		expList.addAll(new PunctuationSelect(keywordList, textList).select());		// d.标点符号
		expList.addAll(new SameTypeSelect(keywordList, textList).select());			// e.搜索词之间是否还有同样kw-type词		
		expList.addAll(new SentenceLengSelect(keywordList, textList).select());		// f.句子的长度
		expList.addAll(new SentimentSelect(keywordList, textList, 8).select());		// g.情感词距离
		expList.addAll(new KeywordSentimentSelect(keywordList, textList, 4).select());		// h.非情感词和情感词距离		
	*/
		return expList;
	}
	
	/**
	 * 获取关键词分词
	 * @param weibo
	 * @return
	 */
	public static List<Participle> getKeyWordList(Weibo weibo) {
		List<Participle> keywordList = new ArrayList<Participle>();
		String[] split = weibo.getKeyWords().split(" ");
		if (split != null && split.length > 0) {
			for (String word : split) {
				Participle sf = new Participle(word);
				String[][] posText = Segment.ICTCLAS_WordProcessWithPos(word);
				if (posText.length == 1) {
					sf.setPosTag(posText[0][1]);	// 设置词性
				}
				keywordList.add(sf);
			}
		}		
		return keywordList;
	}
	/**
	 * 获取正文的分词
	 * @param weibo
	 * @return
	 */
	public static List<Participle> getTextList(Weibo weibo) {
		List<Participle> textList = new ArrayList<Participle>();
		String[][] posText = Segment.ICTCLAS_WordProcessWithPos(weibo.getContent());
		for (int i = 0; i < posText.length; i++) {
			Participle sf = new Participle(posText[i][0]);
			sf.setPosTag(posText[i][1]);	// 设置词性
			textList.add(sf);
		}	
		return textList;
	}

	
	/**
	 * 读取已标签的文件
	 * @param fileName
	 * @return 
	 * @throws IOException
	 */
	public static List<Weibo> getWeiboListFormFile(String fileName, int classifierNum){		
		List<Weibo> weiboList = new ArrayList<Weibo>();
		List<String> weiboStrings = new ArrayList<String>();
		weiboStrings = FileHandler.readFileToList(fileName);
		int count = 0;
		int weiboID = 1;
		while (!weiboStrings.isEmpty()) {
			String line = weiboStrings.get(0);
			weiboStrings.remove(0);
			if (line == null)
				break;
			if (line.length() <= 1)
				continue;
			String[] split = line.split("\t");	//格式：关键字\t正文内容\t标签
			if (split.length >= 3) {
				weiboList.add(new Weibo(split[0], split[1], split[2], weiboID, classifierNum));
				weiboID++;
			} else {
				StackTraceElement ste2 = new Throwable().getStackTrace()[0];
				System.err.println(ste2.getFileName() + ": Line " + ste2.getLineNumber());
				System.err.println("格式错误：+\t第" + count + "行，只有" + split.length + "个字符串");
				return null;
			}
			count++;
		}
		return weiboList;
	}
	/**
	 * 读取已标签的文件
	 * @param fileName
	 * @return 
	 * @throws IOException
	 */
	public static List<Weibo> getWeiboListFormFileNoLabel(String fileName){		
		List<Weibo> weiboList = new ArrayList<Weibo>();
		List<String> weiboStrings = new ArrayList<String>();
		weiboStrings = FileHandler.readFileToList(fileName);
		int count = 0;
		int weiboID = 1;
		while (!weiboStrings.isEmpty()) {
			String line = weiboStrings.get(0);
			weiboStrings.remove(0);
			if (line == null)
				break;
			if (line.length() <= 1)
				continue;
			String[] split = line.split("\t");	//格式：关键字\t正文内容\t标签
			if (split.length >= 2) {
				weiboList.add(new Weibo(split[0], split[1], "1", weiboID, 1));
				weiboID++;
			} else {
				StackTraceElement ste2 = new Throwable().getStackTrace()[0];
				System.err.println(ste2.getFileName() + ": Line " + ste2.getLineNumber());
				System.err.println("格式错误：+\t第" + count + "行，只有" + split.length + "个字符串");
				return null;
			}
			count++;
		}
		return weiboList;
	}
	
	/**
	 * 读取已标签的文件
	 * @param fileName
	 * @return 
	 * @throws IOException
	 */
	public static List<Weibo> getWeiboListFormFileWithID(String fileName, int classifierNum){		
		List<Weibo> weiboList = new ArrayList<Weibo>();
		List<String> weiboStrings = new ArrayList<String>();
		weiboStrings = FileHandler.readFileToList(fileName);
		int count = 0;
		int weiboID = 1;
		while (!weiboStrings.isEmpty()) {
			String line = weiboStrings.get(0);
			weiboStrings.remove(0);
			if (line == null)
				break;
			if (line.length() <= 1)
				continue;
			String[] split = line.split("\t");	//格式：关键字\t正文内容\t标签
			if (split.length == 4) {
				weiboList.add(new Weibo(split[1].trim(), split[2].trim(), split[3].trim(), Integer.parseInt(split[0].trim()), classifierNum));
				weiboID++;;
			} else {
				StackTraceElement ste2 = new Throwable().getStackTrace()[0];
				System.err.println(ste2.getFileName() + ": Line " + ste2.getLineNumber());
				System.err.println("格式错误：+\t第" + count + "行，只有" + split.length + "个字符串");
			}
			count++;
		}
		return weiboList;
	}
	
	/**
	 * 对微博列表做分词,需要输出到文件 
	 * @param weiboList
	 * @return
	 */
	public static List<Weibo> segmentWeiboList(List<Weibo> weiboList, String path, String type) {
	//	String unsegmentFileNameString = path + type + "tmp_unsegment.txt";
		String segmentedFileNameString = path + type + "tmp_segmented.txt";
		List<String> contentList = new ArrayList<String>(weiboList.size());
		for (Weibo weibo : weiboList) {
			contentList.add(StrHandle.removeAt(weibo.getContent()));
		}
	//	WeiboMenagment.writeWeiboContentToFile(weiboList, unsegmentFileNameString); 		// 保存正文内容
		List<String> segmentedList = Segment.ICTCLAS_ParagraphProcess(contentList); 	// 对正文件内容做分词
	//	List<String> segmentedList = Segment.Jeasy_ParagraphProcess(contentList); 	// 对正文件内容做分词
		

	//	Set<String> termSet = new HashSet<String>(FileHandler.readFileToList("c_stoplist.txt"));
		
		// 前缀树合并分词
/*		Retrieval rt = new Retrieval("./conf/ICTCLAS50/csmt_userdict.txt");
		
		segmentedList = rt.retrieval(segmentedList,	false);*/
		
		for (int i = 0; i < segmentedList.size(); i++) {
			String[] split = segmentedList.get(i).split(" ");
			if (split.length > 0) {
				List<String> segmented = new ArrayList<String>();
				for (int j = 0; j < split.length; j++) {
				/*	if (valid(split[j])) {
						segmented.add(split[j]);
					}*/
					String word = split[j];
		/*			if (!termSet.contains(word) && word.length() > 0 && word.length() < 10 && !((word.getBytes()[0] >= 65 && word.getBytes()[0] <= 90) || 
							(word.getBytes()[0] >= 97 && word.getBytes()[0] <= 122))
							&& !(word.getBytes()[0] >= 48 && word.getBytes()[0] <= 57)) {
						segmented.add(word);
					}*/
					segmented.add(word);
				}
				weiboList.get(i).setSegmented(segmented);
			}
		}
		FileHandler.writeListToFile(segmentedList, segmentedFileNameString);
		return weiboList;
	}

	/**
	 * 对微博列表做分词
	 * @param weiboList 微博列表
	 * @return
	 */
	public static List<Weibo> segmentWeiboList(List<Weibo> weiboList) {		
	//	Segment.init();
		for (int i = 0; i < weiboList.size(); i++) {
			String[]  segmenteds = Segment.getWordSegment(weiboList.get(i).getContent());
		//	String[]  segmenteds = Segment.Jeasy_WordProcess(weiboList.get(i).getContent());
			List<String> segmentedList = new ArrayList<String>();
			for (String s : segmenteds){
				if (valid(s)) {
					segmentedList.add(s);
				}
			}
			weiboList.get(i).setSegmented(segmentedList);
		}
		return weiboList;
	}
	
	public static List<Weibo> segmentWeiboList(List<Weibo> weiboList, boolean isITCCLAS) {		
		//	Segment.init();
			for (int i = 0; i < weiboList.size(); i++) {
				String[]  segmenteds = isITCCLAS ? Segment.getWordSegment(weiboList.get(i).getContent()) :
					Segment.Jeasy_WordProcess(weiboList.get(i).getContent());
			//	String[]  segmenteds = Segment.Jeasy_WordProcess(weiboList.get(i).getContent());
				List<String> segmentedList = new ArrayList<String>();
				for (String s : segmenteds){
					if (valid(s)) {
						segmentedList.add(s);
					}
				}
				weiboList.get(i).setSegmented(segmentedList);
			}
			return weiboList;
		}

//	private static Pattern pattern = Pattern.compile("([\\d\\w]+)|([  　]+)");	// 过滤数字、字母
	private static Pattern pattern = Pattern.compile("([  　]+)");	// 过滤数字、字母
	/**
	 * 判断某些关键字是否有效，即过滤掉某些关键字
	 * @param word
	 * @return
	 */
	public static boolean valid(String word){
		word = word.trim();
		if(word.length()==0)
			return false;
		Matcher m = pattern.matcher(word);
		return !m.find();
	}
	
	/**
	 * 将微博列表的正文内容写到文件里
	 * @param weiboList
	 * @param contentFileName
	 */	
	public static void writeWeiboContentToFile(List<Weibo> weiboList, String contentFileName) {
		List<String> weiboContentList = new ArrayList<String>();
		for (Weibo weibo : weiboList) {
			weiboContentList.add(weibo.getContent());
		}
		FileHandler.writeListToFile(weiboContentList, contentFileName);
	}

	/**
	 * 将微博列表的正文内容写到文件里
	 * @param weiboList
	 * @param contentFileName
	 */	
	public static void writeWeiboFeatureToFile(List<Weibo> weiboList, String featureFileName) {
		List<String> weiboContentList = new ArrayList<String>();
		for (Weibo weibo : weiboList) {
			String string = "";
			for (WeiboFeature feature : weibo.getFeatureList()) {
				string += feature.getWord() + " ";			
			}
			weiboContentList.add(string);
		}
		FileHandler.writeListToFile(weiboContentList, featureFileName);
	}

	/**
	 * 将微博列表的正文内容写到文件里
	 * @param weiboList
	 * @param contentFileName
	 */	
	public static void writeWeiboSegmentToFile(List<Weibo> weiboList, String featureFileName) {
		List<String> weiboContentList = new ArrayList<String>();
		for (Weibo weibo : weiboList) {
			String string = "" + weibo.getKeyWords() + "\t" + weibo.getLabel() + "\t";
			for (String word : weibo.getSegmented()) {
				string += word + " ";			
			}
			weiboContentList.add(string);
		}
		FileHandler.writeListToFile(weiboContentList, featureFileName);
	}
	
	/**
	 * 将微博列表的正文内容写到文件里
	 * @param weiboList
	 * @param contentFileName
	 */	
	public static void writeWeiboToFile(List<Weibo> weiboList, String weiboFileName) {
		List<String> weiboContentList = new ArrayList<String>();
		for (Weibo weibo : weiboList) {

/*			String label = weibo.getLabel();
			if (label.equals("0")) {
				label = "3";
			} else if (label.equals("1")) {
				label = "2";
			}
			else {
				System.err.println("WeiboMenagment.writeWeiboToFile()" + label);
			}*/

			weiboContentList.add(weibo.getKeyWords() + "\t"
					+ weibo.getContent() + "\t" + weibo.getLabel());
		/*	weiboContentList.add(weibo.getKeyWords() + "\t"
					+ weibo.getContent() + "\t" + label);*/
		}
		FileHandler.writeListToFile(weiboContentList, weiboFileName);
	}
	
	/**
	 * 将微博列表的正文内容写到文件里
	 * @param weiboList
	 * @param contentFileName
	 */	
	public static void writeWeiboToFileWithID(List<Weibo> weiboList, String weiboFileName) {
		List<String> weiboContentList = new ArrayList<String>();
		for (Weibo weibo : weiboList) {
			weiboContentList.add(String.format("%4d", weibo.getID()) + "\t"
					+ weibo.getKeyWords() + "\t" + weibo.getContent() + "\t"
					+ weibo.getLabel());
		}
		FileHandler.writeListToFile(weiboContentList, weiboFileName);
	}
	
	/**
	 * 将微博列表的正文内容写到文件里
	 * @param weiboList
	 * @param contentFileName
	 */	
	public static void writeLibsvmInputToFile(List<Weibo> weiboList, String libsvmInputFileName) {
		List<String> libsvmInputList = new ArrayList<String>();
		for (Weibo weibo : weiboList) {
			String libsvmInputString = weibo.getCategoryTypeToString();
			for (WeiboFeature feature : weibo.getFeatureList()) {
				libsvmInputString += "\t" + feature.getID() + ":" + feature.getValue();
			}
			libsvmInputList.add(libsvmInputString);
		}
		FileHandler.writeListToFile(libsvmInputList, libsvmInputFileName);
	}

	/**
	 * 获取已赋值的每个特征值作为libsvm输入文件
	 * @param weiboList
	 * @return
	 */
	public static List<String> getLibsvmInputList(List<Weibo> weiboList) {
		List<String> libsvmInputList = new ArrayList<String>();
		for (Weibo weibo : weiboList) {	
			List<WeiboFeature> featureList = new ArrayList<WeiboFeature>();
			featureList = weibo.getFeatureList();
			String writeString = "" + Weibo.cagegoryTypeToInt(weibo.getCategoryType());
			for (WeiboFeature feature : featureList) {
			//	writeString += "\t" + feature.getID() + ":" + feature.getValue();
				writeString += String.format("\t%d:%f", feature.getID(), feature.getValue());
			}
			libsvmInputList.add(writeString);
		}
		return libsvmInputList;
	}
	
	/**
	 * 从文件里获取分词列表
	 * @param segmentedFileName
	 * @return
	 */
	public static List<String>  getSegmentedFormFile(String segmentedFileName) {
		List<String> segmentedList = new ArrayList<String>();
		segmentedList = FileHandler.readFileToList(segmentedFileName);
		return segmentedList;
	}
	public static void printWeboList(List<Weibo> weiboList)
	{
		int count=0;
		for (Weibo weibo : weiboList) {
			System.out.println(weibo.getCategoryType());
			count++;
		}
		System.out.println("printWeboList, count: " + count);
	}
}
