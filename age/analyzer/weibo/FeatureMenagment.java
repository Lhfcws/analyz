package com.yeezhao.analyz.age.analyzer.weibo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.yeezhao.analyz.age.analyzer.feature.assignment.AssignmentType;
import com.yeezhao.analyz.age.analyzer.utility.FileHandler;
import com.yeezhao.analyz.age.analyzer.weibo.Weibo.CATEGORY_TYPE;


public class FeatureMenagment {

	/**
	 * 从文件中读取特征词
	 * @param featureWordFileName
	 * @return
	 */
	public static List<StaticsticFeature> getFeatureFromFile(String featureFileName) {
		List<StaticsticFeature> featureList = new ArrayList<StaticsticFeature>();
		List<String> featureStrings = FileHandler.readFileToList(featureFileName);
		int count = 0;
		while (!featureStrings.isEmpty()){
			String line = featureStrings.get(0);
			featureStrings.remove(0);
			if (line == null)
				break;
			String[] split=line.split("\t");
			if (split.length>0) {
				String word = split[0];
				int ID = Integer.parseInt(split[1]);
				double idf = Double.parseDouble(split[2]);
				double IG = Double.parseDouble(split[3]);
				featureList.add(new StaticsticFeature(word, ID, idf, IG));
				
			/*	int type = Integer.valueOf(split[0]);
				String word = split[1];
				int ID = Integer.parseInt(split[2]);
				double idf = Double.parseDouble(split[3]);
				double IG = Double.parseDouble(split[4]);
				StaticsticFeature sf = new StaticsticFeature(word, ID, idf, IG, type);				
				featureList.add(sf);*/
			}
			else {
				StackTraceElement ste2 = new Throwable().getStackTrace()[0];
				System.err.println(ste2.getFileName() + ": Line " + ste2.getLineNumber());
				System.err.println("格式错误：+\t第"+count+"行，只有"+split.length+"个字符串");
			}
			count++;
		}
		return featureList;
	}
	/**
	 * 选取微博列表的特征
	 * @param weiboList
	 * @return
	 */
	public static List<StaticsticFeature> selectFeature(List<Weibo> weiboList) {
		List<StaticsticFeature> featureList = new ArrayList<StaticsticFeature>();
		
		Map<String,StaticsticFeature> word2featrueMap = new HashMap<String,StaticsticFeature>();

		int posNum=0;	//正类数目
		int negNum=0;	//负类数目
		int allNum=0;	//总样本数目
		
		//1.计算样本正负类的数目
		for (Weibo weibo: weiboList) {
			if (weibo.getCategoryType() == CATEGORY_TYPE.NEG){
				negNum++;
			} else if (weibo.getCategoryType() == CATEGORY_TYPE.POS){
				posNum++;
			}
			else {
				StackTraceElement ste2 = new Throwable().getStackTrace()[0];
				System.err.println(ste2.getFileName() + ": Line " + ste2.getLineNumber());
				System.err.println("出现没有的分类！");
			}
		}
		allNum = negNum + posNum;

		//2.过滤重复的特征
		for (Weibo weibo : weiboList) {
			List<String> words = weibo.getSegmented();
		//	List<String> words = weibo.getAllSegmentedList();
			Set<String> fliterWordSet = new HashSet<String>();
			for (String word : words) { 				
				fliterWordSet.add(word);
			}
			for (String word: fliterWordSet) {				
				if (word2featrueMap.containsKey(word)){ //包含该分词，则修改之				
					word2featrueMap.get(word).classify(weibo.getCategoryType());	//归类
				}
				else {//不包含该分词，则新建				
					word2featrueMap.put(word, new StaticsticFeature(word, posNum, negNum,weibo.getCategoryType()));
				}
			}
		}		


		//4.计算每个特征的CHI值和IG值
		for (Map.Entry<String,StaticsticFeature> w2f : word2featrueMap.entrySet()) {
			w2f.getValue().computeCHI();
			w2f.getValue().computeIG();
			w2f.getValue().computeMI();
		}

		ArrayList<Map.Entry<String, StaticsticFeature>> list = new ArrayList<Map.Entry<String, StaticsticFeature>>(word2featrueMap.entrySet());
		
		//根据CHI值降序排序
		Collections.sort(list, new FeatureCHIComparator());
		
		//根据IG值降序排序
//		Collections.sort(list, new FeatureIGComparator());
		
		//根据MI值降序排序
//		Collections.sort(list, new FeatureMIComparator());
		
		final int maxFeatureCount = 2000; // 特征值数目上限
		int featureCount = 1;

		//5.根据排序，选取前N条且符合阀值的特征
		for (Map.Entry<String,StaticsticFeature> w2f : list) {
			StaticsticFeature f = w2f.getValue();
					
			//设置数目阀值，过滤少于总样本数0.5%的特征
/*			int minNum = (int) (0.5 * allNum / 100);
			if (f.getA() < minNum && f.getB() < minNum) continue;
*/
			if (f.getA() + f.getB() <= 1) continue;
/*			double p_pos = (double) f.getA() / (f.getA() + f.getB());
			double k = 0.15;
			if (p_pos > k && p_pos < (1 - k)) {
				// continue;
			}*/

			double idf = Math.log((double) allNum / (double) (f.getA() + f.getB()));
			f.setIdf(idf); // 设置IDF值，用于特征赋值
			f.setID(featureCount);
			f.setAssignmentType(0);

			featureList.add(f);
			if (featureCount++ >= maxFeatureCount) break;
		}
	//	featureList.addAll(addDirectFeature(weiboList, featureList.size()));
		
		return featureList;		
	}
	
	/**
	 * 增加直接赋值的特征
	 * @param weiboList
	 * @param lastSize
	 * @return
	 */
/*	private static List<StaticsticFeature> addDirectFeature(List<Weibo> weiboList, int lastSize) {
		List<StaticsticFeature> featureList = new ArrayList<StaticsticFeature>();
		List<String> wordSet = new ArrayList<String>();
		for (Weibo weibo : weiboList) {
			List<Participle> partList = weibo.getPartList(AssignmentType.DIRECT);
			for (Participle p : partList) {
				if (!wordSet.contains(p.getWord())) {					
					wordSet.add(p.getWord());
				}
			}
		}
		int id = lastSize;
		for (String word : wordSet) {
			StaticsticFeature sf = new StaticsticFeature(word, ++id, 0, 0, 1);
			featureList.add(sf);
		}		
		return featureList;
	}*/
	
	/**
	 * 将特征列表输出到文件里
	 * @param featureList
	 * @param featureWordFileName
	 */
	public static void writeFeatureWordToFile(List<StaticsticFeature> featureList, String featureWordFileName) {
		List<String> featureStrings = new ArrayList<String>();
		for (StaticsticFeature sf : featureList) {
			String writeString = sf.getWord() + "\t" + sf.getID() + "\t" + sf.getIdf() + "\t" + sf.getIG();
/*	//		String writeString = String.format("%d\t%s\t%d\t%.10f\t%.10f\t%.10f\t%d\t%d\t%d\t%d",
					sf.getAssignmentType(), sf.getWord(), sf.getID(), sf.getIdf(),
					sf.getIG(), sf.getCHI(), 
					sf.getA(), sf.getB(), sf.getC(), sf.getD());*/
			featureStrings.add(writeString);
		}
		FileHandler.writeListToFile(featureStrings, featureWordFileName);
	}
		
	/**
	 * 为微博列表的每个特征赋值，TF-IDF
	 * @param weibo
	 * @param featureList
	 * @param word2IDMap
	 */
	public static void setWeiboFeatureValue(Weibo weibo, List<StaticsticFeature> featureList, Map<String,Integer> word2IDMap) {

		Map<String, FeatureCount> word2CountMap = new HashMap<String, FeatureCount>(); // 特征与其ID的映射
		for (String word : weibo.getSegmented()) {// 记数该特征出现在次数
	//	for (String word : weibo.getAllSegmentedList()) {// 记数该特征出现在次数
			if (word2IDMap.containsKey(word)) {
				if (word2CountMap.containsKey(word)) { // 包含该分词，则修改之
					word2CountMap.get(word).update(); // 更新，频数加一
				} else {// 不包含该分词，则新建
					word2CountMap.put(word, new FeatureCount(word, word2IDMap.get(word)));
				}
			}
		}

		// 根据ID升序排序
		ArrayList<Map.Entry<String, FeatureCount>> list = new ArrayList<Map.Entry<String, FeatureCount>>(word2CountMap.entrySet());
		Collections.sort(list, new WordIDComparator());
		// 对每个特征进行赋值
		double sumWeight = 0;
		List<WeiboFeature> newFeatureList = new ArrayList<WeiboFeature>();
		for (Map.Entry<String, FeatureCount> entry : list) {
			int count = entry.getValue().getCount();
			int id = entry.getValue().getID();
			double tf = (double) count / weibo.getSegmented().size(); // 计算tf
		//	double tf = (double) count / weibo.getAllSegmentedList().size(); // 计算tf
			double idf = featureList.get(id - 1).getIdf(); // 计算idf
			double IG = featureList.get(id - 1).getIG();
			double weight = tf * idf * IG;
			sumWeight += weight * weight;
			entry.getValue().setWeight(weight);
		}
		sumWeight = Math.sqrt(sumWeight);
		for (Map.Entry<String, FeatureCount> entry : list) {
			double featrueValue = entry.getValue().getWeight() / sumWeight; // 特征值的赋值
			featrueValue = 1;
			entry.getValue().setValue(featrueValue);
			WeiboFeature f = new WeiboFeature(entry.getValue().getWord(), entry
					.getValue().getID(), featrueValue);
			newFeatureList.add(f);
		}
	//	newFeatureList.addAll(setExpFeatureValure(weibo, featureList, word2IDMap));
		weibo.setFeatureList(newFeatureList);
	}
	
	/**
	 * 增加额外的
	 * @param weibo
	 * @param featureList
	 * @param word2IDMap
	 * @return
	 */
	private static List<WeiboFeature> setExpFeatureValure(Weibo weibo, List<StaticsticFeature> featureList, Map<String,Integer> word2IDMap) {
		List<WeiboFeature> expFeatureList = new ArrayList<WeiboFeature>();

		List<Participle> partList = weibo.getPartList(AssignmentType.DIRECT);
		for (Participle p : partList) {
			WeiboFeature f = new WeiboFeature(p.getWord(), word2IDMap.get(p.getWord()), p.getValue());
			expFeatureList.add(f);			
		}
		return expFeatureList;
	}
	
	/**
	 * 为微博列表的每个特征赋值，TF-IDF
	 * @param weiboList
	 * @param featureList
	 */
	public static void setWeiboFeatureValue(List<Weibo> weiboList, List<StaticsticFeature> featureList) {

		Map<String,Integer> word2IDMap = new HashMap<String,Integer>();	//特征与其ID的映射

		for (StaticsticFeature f : featureList) {
			word2IDMap.put(f.getWord(), f.getID());
		}
		for (Weibo weibo : weiboList) {
			FeatureMenagment.setWeiboFeatureValue(weibo, featureList, word2IDMap);
		}
	}
	/**
	 * 为微博列表的每个特征赋值,布尔型
	 * @param weiboList
	 * @param featureList
	 */
	public static void setWeiboFeatureValue2(List<Weibo> weiboList, List<StaticsticFeature> featureList)
	{

		Map<String,Integer> word2IDMap = new HashMap<String,Integer>();	//特征与其ID的映射

		for (StaticsticFeature f : featureList) {
			word2IDMap.put(f.getWord(), f.getID());
		}
		
/*		Set<String> brandWordSet = getBrandWordSet();				//品牌词库
		Set<String> negSentimentWordSet = getSentimentWordSet(0);	//负情感词
		Set<String> posSentimentWordSet = getSentimentWordSet(1);	//正情感词
*/		
		for (Weibo weibo: weiboList) {	
			Map<String,FeatureCount> word2CountMap = new HashMap<String,FeatureCount>();	//特征与其ID的映射
			for (String word: weibo.getSegmented()) //记数该特征出现在次数
			{
/*				//使用品牌词
				if (useBrandWord) {
					if (brandWordSet.contains(word)) {
						word = brandWordsString;
					}
				}
				if (useSentimentWord) {
					if (negSentimentWordSet.contains(word)) {
						word = negSentimentWordsString;
					}
					if (posSentimentWordSet.contains(word)) {
						word = posSentimentWordsString;
					}
				}*/

				if (word2IDMap.containsKey(word)) {		
					if (word2CountMap.containsKey(word)) //包含该分词，则修改之
					{
						word2CountMap.get(word).update();	//更新，频数加一
					}
					else //不包含该分词，则新建
					{
						word2CountMap.put(word, new FeatureCount(word,word2IDMap.get(word)));
					}
				}
			}

			//根据ID升序排序
			ArrayList<Map.Entry<String, FeatureCount>> list = new ArrayList<Map.Entry<String, FeatureCount>>(word2CountMap.entrySet());
			Collections.sort(list, new WordIDComparator());
			//对每个特征进行赋值
			List<WeiboFeature> newFeatureList = new ArrayList<WeiboFeature>();
			for (Map.Entry<String, FeatureCount> entry : list) {
				WeiboFeature f = new WeiboFeature(entry.getValue().getWord(), entry.getValue().getID(), 1);		// 布尔型赋值
				newFeatureList.add(f);
			}
			weibo.setFeatureList(newFeatureList);
		}		
	}
}
/**
 * 每特征对应频数
 * @author user
 *
 */
class FeatureCount
{
	private String word;	//特征词
	private int count;		//频数
	private int ID;			//
	private double value;	//特征值
	private double weight;	//权重
	FeatureCount(String word, int id) {
		count = 1;
		this.word = word;
		this.ID = id;
	}
	public void update() {
		count++;
	}
	public int getCount() {
		return count;
	}
	public int getID() {
		return ID;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	public double getWeight() {
		return weight;
	}
	public void setWeight(double weight) {
		this.weight = weight;
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
}

//CHI值排序
class FeatureCHIComparator implements Comparator<Map.Entry<String, StaticsticFeature>> {
	public int compare(Map.Entry<String, StaticsticFeature> o1,
			Map.Entry<String, StaticsticFeature> o2) {
		if ((o2.getValue().getCHI() - o1.getValue().getCHI())>0) {	//降序
			return 1;			
		}
		else return -1;
	}
}
//IG值排序
class FeatureIGComparator implements Comparator<Map.Entry<String, StaticsticFeature>> {
	public int compare(Map.Entry<String, StaticsticFeature> o1,
			Map.Entry<String, StaticsticFeature> o2) {
		if ((o2.getValue().getIG() - o1.getValue().getIG())>0) {	//降序
			return 1;			
		}
		else return -1;
	}
}
//MI值排序
class FeatureMIComparator implements Comparator<Map.Entry<String, StaticsticFeature>> {
	public int compare(Map.Entry<String, StaticsticFeature> o1,
			Map.Entry<String, StaticsticFeature> o2) {
		if ((o2.getValue().getMI() - o1.getValue().getMI())>0) {	//降序
			return 1;			
		}
		else return -1;
	}
}
//特征ID排序
class WordIDComparator implements Comparator<Map.Entry<String, FeatureCount>> {

	public int compare(Map.Entry<String, FeatureCount> o1,
			Map.Entry<String, FeatureCount> o2) {
		if ((o2.getValue().getID() - o1.getValue().getID())<0) {	//升序
			return 1;			
		}
		else return -1;
	}
}
//词频值排序
class FeatureTFComparator implements Comparator<Map.Entry<String, StaticsticFeature>> {
	public int compare(Map.Entry<String, StaticsticFeature> o1,
			Map.Entry<String, StaticsticFeature> o2) {
		if (((o2.getValue().getA() + o2.getValue().getB()) 
				- (o1.getValue().getA() + o1.getValue().getB()))>0) {	//降序
			return 1;			
		}
		else return -1;
	}
}
//B/A排序
class FeatureBAComparator implements Comparator<Map.Entry<String, StaticsticFeature>> {
	public int compare(Map.Entry<String, StaticsticFeature> o1,
			Map.Entry<String, StaticsticFeature> o2) {
		//升序
		if (o2.getValue().getB() == 0 && o1.getValue().getB() == 0) {	//全为0时
			if ((o2.getValue().getA() - o1.getValue().getA())>0) {	//升序
				return 1;			
			}
			else return -1;		
		} else{
			if ((1.0 * o2.getValue().getB() / o2.getValue().getA()
					- 1.0 * o1.getValue().getB() / o1.getValue().getA())<0) {	//降序
				return 1;			
			}
			else return -1;
		}
	}
}
//特征值排序
class WordValueComparator implements Comparator<Map.Entry<String, FeatureCount>> {

	public int compare(Map.Entry<String, FeatureCount> o1,
			Map.Entry<String, FeatureCount> o2) {
		if ((o2.getValue().getValue() - o1.getValue().getValue())>0) {	//降序
			return 1;			
		}
		else return -1;
	}
}
