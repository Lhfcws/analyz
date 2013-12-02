package com.yeezhao.analyz.age.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 属性特征
 * @author Administrator
 *
 */
public class AttributeFeature {
	private String word;
	private Integer[] sampleCounts;	// 特征在各样本中的数量
	private Integer[] belongs;	// 
	private double CHI; 		// 卡方统计量,用于特征选取
	private int tf; 			// TF系数，用于特征赋值
	private double idf; 		// IDF系数，用于特征赋值
	private List<Abcd> abcdList;
	private int allSampleCount;
	private int allBelong;
	
	public AttributeFeature(String word, Integer[] sampleCounts, int allSampleCount) {
		super();
		this.word = word;
		this.sampleCounts = sampleCounts;
		this.belongs = new Integer[sampleCounts.length];
		Arrays.fill(this.belongs, 0);
		this.allSampleCount = allSampleCount;
		this.abcdList = new ArrayList<Abcd>();
	}
	/**
	 * 对样本数目加1
	 * @param cIndex
	 */
	public void addCount(int cIndex) {
		belongs[cIndex]++;
	}
	public void computeAbcd() {
		abcdList = new ArrayList<Abcd>();
		for (int i = 0; i < sampleCounts.length; i++) {
			abcdList.add(new Abcd(sampleCounts, belongs, i));
		}
		computeAllBelong();
	}
	public void computeAllBelong() {
		allBelong = 0;
		for (int i = 0; i < belongs.length; i++) {
			allBelong += belongs[i];
		}
	}
	/**
	 * 计算CHI值
	 */
	public void computeCHI() {
		double maxValue = Double.NEGATIVE_INFINITY;
		for (Abcd abcd : abcdList) {
			maxValue = Math.max(abcd.getCHI(allSampleCount), maxValue);
		}
		CHI = maxValue;
	}
	
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public Integer[] getSampleCounts() {
		return sampleCounts;
	}
	public void setSampleCounts(Integer[] sampleCounts) {
		this.sampleCounts = sampleCounts;
	}
	public Integer[] getBelongs() {
		return belongs;
	}
	public void setBelongs(Integer[] belongs) {
		this.belongs = belongs;
	}
	public int getAllBelong() {
		return allBelong;
	}
	public void setAllBelong(int allBelong) {
		this.allBelong = allBelong;
	}
	public double getCHI() {
		return CHI;
	}
	public void setCHI(double cHI) {
		CHI = cHI;
	}
	public int getTf() {
		return tf;
	}
	public void setTf(int tf) {
		this.tf = tf;
	}
	public double getIdf() {
		return idf;
	}
	public void setIdf(double idf) {
		this.idf = idf;
	}
	public List<Abcd> getAbcdList() {
		return abcdList;
	}
	public void setAbcdList(List<Abcd> abcdList) {
		this.abcdList = abcdList;
	}
	public int getAllSampleCount() {
		return allSampleCount;
	}
	public void setAllSampleCount(int allSampleCount) {
		this.allSampleCount = allSampleCount;
	}	
}

class Abcd {
	private int c_t;	// 
	private int c_nt;	// 
	private int nc_t;	// 
	private int nc_nt;	// 
	public Abcd(Integer[] samples, Integer[] belongs, int cIndex) {
		super();
		init(samples, belongs, cIndex);
	}
	private void init(Integer[] samples, Integer[] belongs, int cIndex) {
		c_t = 0;
		c_nt = 0;
		nc_t = 0;
		nc_nt = 0;
		for (int i = 0; i < belongs.length; i++) {
			if (i == cIndex) {	// 属于该类
				c_t = belongs[i];
				c_nt = samples[i] - belongs[i];				
			} else {	// 不属于该类
				nc_t += belongs[i];
				nc_nt += (samples[i] - belongs[i]);	
			}
		}
	}
	/**
	 * 计算CHI值
	 */
	public double getCHI(int sampleCount) {
		int A = c_t;
		int B = c_nt;
		int C = nc_t;
		int D = nc_nt;
		return (double) sampleCount * Math.pow((A * D - C * B), 2)
				/ ((double) (A + C) * (B + D) * (A + B) * (C + D)); // 防溢出，先转化为double型
	}

	public int getC_t() {
		return c_t;
	}
	public void setC_t(int c_t) {
		this.c_t = c_t;
	}
	public int getC_nt() {
		return c_nt;
	}
	public void setC_nt(int c_nt) {
		this.c_nt = c_nt;
	}
	public int getNc_t() {
		return nc_t;
	}
	public void setNc_t(int nc_t) {
		this.nc_t = nc_t;
	}
	public int getNc_nt() {
		return nc_nt;
	}
	public void setNc_nt(int nc_nt) {
		this.nc_nt = nc_nt;
	}
	
}