package com.yeezhao.analyz.age.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

import com.yeezhao.analyz.age.analyzer.utility.DeciFormat;
import com.yeezhao.analyz.age.analyzer.utility.FileHandler;
import com.yeezhao.analyz.age.analyzer.weibo.Weibo;
import com.yeezhao.analyz.age.analyzer.weibo.WeiboMenagment;
import com.yeezhao.analyz.age.comparator.AttributeFeatureCHICmp;
import com.yeezhao.analyz.age.comparator.StringHashCmp;

public class SampleFactoryCHI {

	final static private String LABEL_STRING = "class";
	
	public Sample createSample(List<Weibo> weiboList) {
		
		weiboList = WeiboMenagment.segmentWeiboList(weiboList);		

		List<String> wordList = getWordList(weiboList);		
		Set<String> labelSet = new TreeSet<String>(getLabelList(weiboList));
		// 初始属性
		FastVector attributes = getAttributeList(wordList);
		addLabel(attributes, labelSet);

		// 选择的属性
		FastVector selectAttributes = selectAtrr(weiboList, attributes);
		addLabel(selectAttributes, labelSet);
		
		// 数据
		Instances dataset = getDataset(weiboList, selectAttributes);
		
		System.out.println("dataset.numAttributes().size: " + dataset.numAttributes());
		Sample sample = new Sample(dataset);		
		return sample;
	}
	/**
	 * 属性选择
	 * @param weiboList
	 * @param attributes
	 * @return
	 */
	private FastVector selectAtrr(List<Weibo> weiboList, FastVector attributes) {
		Attribute labels = (Attribute)(attributes.lastElement());		
		Integer[] sampleCountList = getSampleCounts(weiboList, labels);		
		Map<String, AttributeFeature> attriFeatureMap = new HashMap<String, AttributeFeature>();		
		Label label = new Label(labels);
		for (Weibo weibo : weiboList) {			
			Set<String> wordSet = new HashSet<String>(weibo.getSegmented());
			for (String word : wordSet) {
				if (!attriFeatureMap.containsKey(word)) {
					attriFeatureMap.put(word, new AttributeFeature(word, sampleCountList, weiboList.size()));
				}
				attriFeatureMap.get(word).addCount(label.getIndex(weibo.getLabel()));
			}
		}
		List<AttributeFeature> attriFeatureList = new ArrayList<AttributeFeature>(attriFeatureMap.values());
		for (AttributeFeature af : attriFeatureList) {
			af.computeAbcd();
			af.computeCHI();
		}
		Collections.sort(attriFeatureList, new AttributeFeatureCHICmp());
		FastVector selectAttris = new FastVector();
		for (AttributeFeature af : attriFeatureList) {
			if (af.getWord().equals(LABEL_STRING)) {
				attriFeatureList.remove(af);
				break;
			}
		}
		attriFeatureList = attriFeatureList.subList(0, Math.min(attriFeatureList.size(), 2000));
		
		saveAttriFeatureList(attriFeatureList, "e:\\age\\attri.txt");
		
		for (AttributeFeature af : attriFeatureList) {
			selectAttris.addElement(new Attribute(af.getWord()));
		}
		return selectAttris;
	}
	
	private void saveAttriFeatureList(List<AttributeFeature> attriFeatureList, String fileName) {
		List<String> saveList = new ArrayList<String>();
		for (AttributeFeature af : attriFeatureList) {
			String line = String.format("%s\t%s\t%s\t%s\t%d", Arrays.asList(af.getSampleCounts()),
					af.getWord(), DeciFormat.format1or3(af.getCHI()), 
					Arrays.asList(af.getBelongs()), af.getAllBelong());
			saveList.add(line);
		}
		FileHandler.writeListToFile(saveList, fileName);
	}

	private Integer[] getSampleCounts(List<Weibo> weiboList, Attribute labels) {
		Label label = new Label(labels);
		Integer[] sampleCounts = new Integer[label.getLabelCount()];
		Arrays.fill(sampleCounts, 0);
		for (Weibo weibo : weiboList) {
			int index = label.getIndex(weibo.getLabel());
			sampleCounts[index]++;
		}
		return sampleCounts;
	}

	private Instances getDataset(List<Weibo> weiboList, FastVector attributes) {
		Instances dataset = new Instances("Test-dataset", attributes, 0);
		Attribute labels = dataset.attribute(dataset.numAttributes() - 1);
		AttributeTool at = new AttributeTool(attributes);
		for (Weibo weibo : weiboList) {
			double[] values = new double[dataset.numAttributes()];
			Arrays.fill(values, 0);
			for (String word : weibo.getSegmented()) {
				Integer index = at.getIndex(word);
				if (index != null) {
					values[index] = 1;
				}
			}
			values[values.length - 1] = labels.indexOfValue(weibo.getLabel());
			Instance inst = new Instance(1.0, values);
			dataset.add(inst);
		}
		return dataset;
	}
/*	private List<String> getWordFromAttributes(FastVector attributes) {
		List<String> wordList = new ArrayList<String>();
		for (int i = 0; i < attributes.size(); i++) {
			Attribute attri = (Attribute)(attributes.elementAt(i));
			wordList.add(attri.name());
		}
		return wordList;
	}*/
	private List<String> getWordList(List<Weibo> weiboList) {
		Set<String> wordSet = new HashSet<String>();
		for (Weibo weibo : weiboList) {
			wordSet.addAll(weibo.getSegmented());
		}
		if (wordSet.contains(LABEL_STRING)) {
			wordSet.remove(LABEL_STRING);
		}
		List<String> wordList = new ArrayList<String>(wordSet);
		
		Collections.sort(wordList, new StringHashCmp());
		
		return wordList;
	}
	private Collection<String> getLabelList(List<Weibo> weiboList) {
		Set<String> labelSet = new TreeSet<String>();
		for (Weibo weibo : weiboList) {
			labelSet.add(weibo.getLabel());
		}
		return labelSet;
	}
		
	private FastVector getAttributeList(Collection<String> wordList) {
		FastVector attributes = new FastVector();
		for (String word : wordList) {
			attributes.addElement(new Attribute(word));
		}
		return attributes;
	}
	private void addLabel(FastVector attributes, Collection<String> labelSet) {
		FastVector labels = new FastVector();
		for (String label : labelSet) {			
			labels.addElement(label);
		}

		Attribute cls = new Attribute(LABEL_STRING, labels);
		
		attributes.addElement(cls);
	}
}
