package com.yeezhao.analyz.age.classifier;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instances;
import weka.core.SerializationHelper;

import com.yeezhao.analyz.age.analyzer.utility.DeciFormat;
import com.yeezhao.analyz.age.analyzer.utility.FileHandler;
import com.yeezhao.analyz.age.analyzer.weibo.Weibo;
import com.yeezhao.analyz.age.data.Label;
import com.yeezhao.analyz.age.data.Sample;
import com.yeezhao.analyz.age.data.SampleCreator;
import com.yeezhao.analyz.age.user.User;
import com.yeezhao.analyz.age.user.UserWeibo;

public class AgeClassifier {

	private Collection<String> selectWords;	// 人工选取的特征

	private Sample modelSample;
	private Classifier modelClassifier; 
	private Evaluation eval; // 

	public AgeClassifier(String dataFileName, String modelFileName, String attriWordFileName) throws Exception {
		selectWords = new ArrayList<String>(FileHandler.readFileToList(attriWordFileName));
		modelSample = getInitSample(dataFileName);
		eval = new Evaluation(modelSample.getInstances());
		System.out.println("modelFileName: " + modelFileName);
		modelClassifier = loadModel(modelFileName);
	}
	public AgeClassifier(String dataFileName, String modelFileName) throws Exception {
		super();
		modelSample = getInitSample(dataFileName);
		eval = new Evaluation(modelSample.getInstances());
		modelClassifier = loadModel(modelFileName);
		selectWords = new ArrayList<String>();
	}
	
	public AgeClassifier(String dataFileName, InputStream modelStream, String attriWordFileName) throws Exception {
		selectWords = new ArrayList<String>(FileHandler.readFileToList(attriWordFileName));
		modelSample = getInitSample(dataFileName);
		eval = new Evaluation(modelSample.getInstances());
		modelClassifier = loadModel(modelStream);
	}
	
	public AgeClassifier(String dataFileName, InputStream modelStream) throws Exception {
		super();
		modelSample = getInitSample(dataFileName);
		eval = new Evaluation(modelSample.getInstances());
		modelClassifier = loadModel(modelStream);
		selectWords = new ArrayList<String>();
	}
	

	public AgeClassifier(String attriWordFileName) {
		selectWords = new ArrayList<String>(FileHandler.readFileToList(attriWordFileName));
	}
/*	private Sample getSample(String fileName) {
		List<Weibo> weibos = getWeiboList(fileName);
		Sample trainSample = new SampleCreator().createSample(weibos);
		trainSample.save("sample.arff");
		return trainSample;
	}*/
	public void saveSample(String fileName) {
		modelSample.save(fileName);
	}
	public Sample getInitSample(String fileName) {
		Sample sample = new Sample(fileName);
		return sample;
	}
	
	public Collection<String> getSelectWords() {
		return selectWords;
	}
	public void setSelectWords(Collection<String> selectWords) {
		this.selectWords = selectWords;
	}
	public Sample getModelSample() {
		return modelSample;
	}
	public void setModelSample(Sample modelSample) {
		this.modelSample = modelSample;
	}
	public Classifier getModelClassifier() {
		return modelClassifier;
	}
	public void setModelClassifier(Classifier modelClassifier) {
		this.modelClassifier = modelClassifier;
	}
	/*	private Evaluation getInitEvaluation(String fileName) throws Exception {
		List<Weibo> weibos = getWeiboList(fileName);
		Sample trainSample = new SampleCreator().createSample(weibos);
		Instances train = trainSample.getInstances();
		return new Evaluation(train);
	}*/
	/**
	 * 对用户年龄进行分类
	 * @param user
	 * @return 返回年龄分类结果：[1、2、3、4]
	 */
	public String classify(User user) {
		List<Weibo> weiboList = getWeiboList(new ArrayList<User>(Arrays.asList(user)));
		Instances predictSample = new SampleCreator().createInstance(modelSample, weiboList);
		List<String> resultList = classify(predictSample);
		if (resultList != null && resultList.size() > 0) {
			return resultList.get(0);
		}
		return null;
	}
	private List<String> classify(Instances predictSample) {
		return predict(modelSample, predictSample);
	}
	private List<String> predict(Sample trainSample, Instances predictSample) {
		Instances train = trainSample.getInstances();
		List<String> resultList = new ArrayList<String>();
		try {
/*			Classifier cls = modelClassifier;
			if (modelClassifier == null) {
				System.err.println("null model!");
				return resultList;
			}
			Evaluation eval = new Evaluation(train);*/
			eval.evaluateModel(modelClassifier, predictSample);
			for (int i = 0; i < predictSample.numInstances(); i++) {
				double pred = modelClassifier.classifyInstance(predictSample.instance(i));
				resultList.add((predictSample.classAttribute().value((int) pred)));
			}	
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		return resultList;
	}
	public void predict(String fileName) {
		List<Weibo> weibos = getWeiboList(fileName);
		weibos = weibos.subList(0, Math.min(weibos.size(), 10000));

		Instances predictSample =new SampleCreator().createInstance(modelSample, weibos);
		List<String> resultList = classify(predictSample);
		if (resultList != null && resultList.size() > 0) {
			for (String string : resultList) {
				System.out.println(string);
			}
		}
		System.out.println("train and predict finish!");
	}
	private void saveModel(Classifier cls, String fileName) {
		try {
			SerializationHelper.write(fileName, cls);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private Classifier loadModel(String fileName) {
		try {
			return (Classifier) SerializationHelper.read(fileName);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private Classifier loadModel(InputStream modelStream) {
		try {
			return (Classifier) SerializationHelper.read(modelStream);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 训练和预测，这里会生成2个文件：
	 * 1、模型文件：NB.model
	 * 2、特征文件：sample.arff
	 * @param fileName
	 */
	public void trainAndPredict(String fileName) {

		// 1.预处理
		List<Weibo> weibos = getWeiboList(fileName);	
		weibos = weibos.subList(0, Math.min(weibos.size(), 10000));		
		
		System.out.println("segment finish!");
				
		List<Weibo> weibos1 = new ArrayList<Weibo>();
		List<Weibo> weibos2 = new ArrayList<Weibo>();
		
		// 2.分割训练和测试样本
		cutWeibo(weibos, weibos1, weibos2);
		
		System.out.println("weibos.size: " + weibos1.size());
		System.out.println("weibos.size: " + weibos2.size());

		// 3. 数据格式转换
		Sample trainSample = new SampleCreator().createSample(weibos1);	// 特征选择
		
		Sample predictSample = new SampleCreator().createSample(trainSample, weibos2);

		System.out.println("start train and predict ");
		
		// 4.训练和测试
		train(trainSample, predictSample, weibos2);
		
		
		trainSample.save("sample.arff");	// 保存特征文件
		System.out.println("train and predict finish!");
	}
	public void saveModel(String fileName) {
		try {
			List<Weibo> weibos = getWeiboList(fileName);
			weibos = weibos.subList(0, Math.min(weibos.size(), 10000));
			Sample trainSample = new SampleCreator().createSample(weibos);
			Instances train = trainSample.getInstances();
			Classifier cls = new NaiveBayes();
			cls.buildClassifier(train);
			System.out.println("saving model file");
			saveModel(cls, "NB.model");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 训练模型
	 * @param trainSample
	 * @param predictSample
	 * @param predictWeibos
	 */
	private void train(Sample trainSample, Sample predictSample, List<Weibo> predictWeibos) {
		Instances train = trainSample.getInstances();
		Instances test = predictSample.getInstances();
		// train classifier
		try {
			Classifier cls = new NaiveBayes(); 	// 选择NB分类器
			cls.buildClassifier(train);
			// 评估
			Evaluation eval = new Evaluation(train);
			eval.evaluateModel(cls, test);
			
			System.out.println(eval.toSummaryString("\nResults\n\n", false));
			saveModel(cls, "NB.model"); // 保存分类模型
			
			//以下内容为显示训练效果之用
			int correct = 0;
			int correctAfterCount = 0;
			int afterCount = 0;
			String[] orgs = new String[test.numInstances()];
			String[] results = new String[test.numInstances()];
			List<String> list1 = new ArrayList<String>();
			List<String> list2 = new ArrayList<String>();
			for (int i = 0; i < test.numInstances(); i++) {
				double pred = cls.classifyInstance(test.instance(i));	// 预测结果

				list1.add(test.instance(i).toString(test.classIndex()));	// 原来标注的结果
				list2.add(test.classAttribute().value((int) pred));			// 预测的结果
			}
			orgs = new String[list1.size()];
			results = new String[list2.size()];
			for (int i = 0; i < orgs.length; i++) {
				orgs[i] = list1.get(i);
			}
			for (int i = 0; i < results.length; i++) {
				results[i] = list2.get(i);
			}
			printStaticstic(orgs, results);

			System.out.println("all count: " + test.numInstances());
			System.out.println("correct count: " + correct + ", " + DeciFormat.percent1(1.0 * correct / test.numInstances()));
			System.out.println("correct afterCount count: " + correctAfterCount);
			System.out.println("error afterCount count: " + afterCount);			
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 打印统计结果
	 * @param orgs
	 * @param results
	 */
	public static void printStaticstic(String[] orgs, String[] results) {
		Set<String> labelSet = new HashSet<String>();
		labelSet.addAll(Arrays.asList(orgs));
		labelSet.addAll(Arrays.asList(results));
		FastVector labels = new FastVector();
		for (String label : labelSet) {			
			labels.addElement(label);
		}
		Label label = new Label(new Attribute("class", labels));
		
		int[][] count = new int[label.getLabelCount() + 1][label.getLabelCount() + 1];
		for (int i = 0; i < orgs.length; i++) {
			int index1 = label.getIndex(orgs[i]);
			int index2 = label.getIndex(results[i]);
			count[index1][index2]++;
			count[count.length - 1][index2]++;
			count[index1][count.length - 1]++;
			count[count.length - 1][count.length - 1]++;
		}
		System.out.println("");
		System.out.println("orgs and results");
		for (int i = 0; i < count.length; i++) {
			for (int j = 0; j < count[i].length; j++) {
				System.out.print(count[i][j] + "\t");
			}
			System.out.println("");
		}
		System.out.println("");
		System.out.println("precision and recall");
		int correctCount = 0;
		for (int i = 0; i < count.length - 1; i++) {
			System.out.println(String.format("%s\t%s\t%s", 
					label.getLabel(i), 
					DeciFormat.percent1(1.0 * count[i][i] / (count[count.length - 1][i])), 	// 准确率
					DeciFormat.percent1(1.0 * count[i][i] / (count[i][count.length - 1]))	// 召回率
					));
			correctCount += count[i][i];
		}

		System.out.println(String.format("all precision: %d / %d = %s", correctCount, orgs.length, DeciFormat.percent1(1.0 * correctCount / orgs.length)));

	}
	private List<Weibo> getWeiboList(List<User> users) {
		List<Weibo> weiboList = new ArrayList<Weibo>();
		for (User user : users) {
			// 微博正文
			StringBuffer sb = new StringBuffer();
			final int limitCount = 50;
			if (user.getWeibos() != null) {
				for (int i = 0; i < Math.min(limitCount, user.getWeibos().size()); i++) {
					UserWeibo userWeibo = user.getWeibos().get(i);
					sb.append(userWeibo.getText());
				}
			}
			// tag
			List<String> segmentList = new ArrayList<String>();
			if (user.getTag() != null) {
				segmentList.addAll(user.getTag());
			}
			Weibo weibo = new Weibo(sb.toString());
			weibo.setKeyWords("keywords");
			weibo.setLabel("1");
			weibo.setSegmented(segmentList);
			
			addAtrri2Weibo(weibo);
			weiboList.add(weibo);
		}
		return weiboList;
	}
	private void addAtrri2Weibo(Weibo weibo) {
		for (String word : selectWords) {
			if (weibo.getContent().contains(word)) {
				weibo.getSegmented().add(word);
			}
		}
	}
	private void addAtrri2Weibo(List<Weibo> weibos) {
		for (Weibo weibo : weibos) {
			addAtrri2Weibo(weibo);
		}
	}
	
	/**
	 * 预处理
	 * @param fileName
	 * @return
	 */
	private List<Weibo> getWeiboList(String fileName) {
		List<String> fileList = FileHandler.readFileToList(fileName);
		List<Weibo> weiboList = new ArrayList<Weibo>();
		
		for (int i = 0; i < fileList.size(); i++) {
			String line = fileList.get(i);
			String[] split = line.split("\t");
			if (split.length == 5) {
				int age = Integer.valueOf(split[2]);
				String text = split[3];

				// 1. 选取tag
				List<String> tagList = new ArrayList<String>();
				if (text.length() > 0) {
					tagList.addAll(Arrays.asList(text.split("[|]")));
				}
				
				// 2. 选取weibo
				StringBuffer sb = new StringBuffer();
				int weiboCount = Integer.valueOf(split[4]);
				final int limitCount = 50;
				int addCount = 0;
				for (int j = 0; j < weiboCount; j++) {
					i++;
					String weiboLine = fileList.get(i);
					String[] textSplit = weiboLine.split("\t");
				//	System.out.println(textSplit.length);
					if (textSplit.length == 3) {
						if (++addCount <= limitCount && textSplit[1].length() > 0) {							
							sb.append(textSplit[2]);
						}
					}
				}
				
				
				Weibo weibo = new Weibo(sb.toString());
				weibo.setKeyWords(split[2]);
				weibo.setLabel(getAgeLabel(age));
				weibo.setSegmented(tagList);
				
				
			/*	if (text .length() == 0) {
					continue;
				}*/
				weiboList.add(weibo);
			} else {
				System.err.println(split.length + "\t" + line);
			}
		}
	//	weiboList = WeiboMenagment.segmentWeiboList(weiboList);
		
		// 3.把weibo和tag整合在一起
		addAtrri2Weibo(weiboList);
		return weiboList;
	}
	private List<Weibo> getWeiboList2(String fileName) {
		List<String> fileList = FileHandler.readFileToList(fileName);
		List<Weibo> weiboList = new ArrayList<Weibo>();
		for (int i = 0; i < fileList.size(); i++) {
			String line = fileList.get(i);
			String[] split = line.split("\t");
			if (split.length == 5) {
				int age = Integer.valueOf(split[2]);
				String text = split[3];
				Weibo weibo = new Weibo(text);
				weibo.setKeyWords(split[2]);
				weibo.setLabel(getAgeLabel(age));
				List<String> segmentList = new ArrayList<String>();
				if (text.length() > 0) {
					segmentList.addAll(Arrays.asList(text.split("[|]")));
				}
				weibo.setSegmented(segmentList);
				
			/*	if (text .length() == 0) {
					continue;
				}*/
				weiboList.add(weibo);
			} else {
				System.err.println(split.length + "\t" + line);
			}
		}
		return weiboList;
	}
	private void cutWeibo(List<Weibo> weibos, List<Weibo> weibos1, List<Weibo> weibos2) {

		int count = weibos.size();
		int[] map = new int[count];
		for (int i = 0; i < count; i++) {
			map[i] = -1;
		}
		for (int i = 0; i < count; i++) // 做随机映射
		{
			int index = (int) (Math.random() * count);
			while (map[index] >= 0) {
				index++;
				index %= count;
			}
			map[index] = i;
		}

		int predictStart = 0;
		int predictEnd = (int) (count * 0.2);
		for (int i = 0; i < count; i++) {
			if (i < predictStart || i > predictEnd) // 训练数据
				weibos1.add(weibos.get(i));
			else
				// 预测数据
				weibos2.add(weibos.get(i));
		}
	}
	/**
	 * 获取年龄标签
	 * @param year
	 * @return
	 */
/*	public static String getAgeLabel(int year) {
		if (year >= 1990) {
			return year >= 2000 ? "1" : "2";
		} else {
			return year >= 1970 ? (year >= 1980 ? "3" : "4") : "5";
		}
	}*/
	public static String getAgeLabel(int year) {
		if (year >= 1988) {
			return year >= 1995 ? "1" : "2";
		} else {
		//	return year >= 1963 ? (year >= 1978 ? "3" : "4") : "5";
			return year >= 1978 ? "3" : "4";
		}
	}		
/*	public static void main(String[] args) {
		for (int i = 1960; i < 2010; i++) {
			System.out.println(i + "\t" + getAgeLabel(i));
			
		}
	}*/
}
