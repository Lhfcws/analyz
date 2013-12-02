package com.yeezhao.analyz.age.analyzer.weibo;

import com.yeezhao.analyz.age.analyzer.weibo.Weibo.CATEGORY_TYPE;


/**
 * 统计的特征词，特征选择和特征赋值时用
 * 
 * @author yunbiao
 * 
 */
public class StaticsticFeature {

	private String word; 		// 词组
	private int ID; 			// 该特征的ID
	private double value;		// 特征的值
	
	private int A; 				// 包含它而且属于正类的文档频数
	private int B; 				// 包含它但不属于正类的文档频数
	private int C; 				// 属于正类但不包含它的文档频数
	private int D; 				// 不属于正类且不包含它的文档频数
	private int sampleNum; 		// 样本正类+负类的总数
	private int samplePosNum;	// 样本正类数目
	private int sampleNegNum;	// 样本负类数目

	private double CHI; 		// 卡方统计量,用于特征选取
	private int tf; 			// TF系数，用于特征赋值
	private double idf; 		// IDF系数，用于特征赋值
	private int df_neg; 		// 在负类中的DF系数，用于特征赋值
	private int df_pos; 		// 在正类中的DF系数，用于特征赋值
	private double IG; 			// 信息增益(Information Gain，IG）
	private double MI; 			// 互信息(Mutual Information, MI)
	
	private int assignmentType;	// 赋值类型

	public StaticsticFeature(String word) {
		this.word = word;
	}
	/**
	 * 构造方法，输出时调用
	 * 
	 * @param word
	 * @param ID
	 * @param value
	 */
	public StaticsticFeature(String word, int ID, double value) {
		super();
		this.word = word;
		this.ID = ID;
		this.value = value;
	}
	/**
	 * 构造方法，用于预测时调用
	 * 
	 * @param word
	 * @param idf
	 * @param IG
	 */
	public StaticsticFeature(String word, int ID, double idf, double IG) {
		super();
		this.word = word;
		this.ID = ID;
		this.idf = idf;
		this.IG = IG;
	}
	/**
	 * 构造方法，用于预测时调用
	 * 
	 * @param word
	 * @param idf
	 * @param IG
	 */
	public StaticsticFeature(String word, int ID, double idf, double IG, int assignmentType) {
		super();
		this.word = word;
		this.ID = ID;
		this.idf = idf;
		this.IG = IG;
		this.assignmentType = assignmentType;
	}

	public StaticsticFeature(String word, int num, int a, int b, int c, int d, CATEGORY_TYPE categoryID) {
		super();
		this.word = word;
		this.sampleNum = num;
		A = a;
		B = b;
		C = c;
		D = d;
		tf = 0;
		classify(categoryID);
	}

	/**
	 * 构造方法，用于训练时调用
	 * 
	 * @param word 关键字
	 * @param posNum 正类的数目
	 * @param negNum 负类的数目
	 */
	public StaticsticFeature(String word, int posNum, int negNum, CATEGORY_TYPE categoryID) {
		super();
		this.word = word;
		this.samplePosNum = posNum;
		this.sampleNegNum = negNum;
		this.sampleNum = posNum + negNum;
		A = 0;
		B = 0;
		C = posNum;
		D = negNum;
		tf = 0;
		classify(categoryID);
	}

	/**
	 * 归类
	 * 
	 * @param classID
	 *            类（1=一类，2=二类）
	 */
	public void classify(CATEGORY_TYPE classID) {
		if (classID == CATEGORY_TYPE.NEG) { // 第一类，负类
			B++;
			D--;
			df_neg++;
		} else if (classID == CATEGORY_TYPE.POS) { // 第二类，正类
			A++;
			C--;
			df_pos++;
		} else {
			StackTraceElement ste2 = new Throwable().getStackTrace()[0];
			System.err.println(ste2.getFileName() + ": Line "
					+ ste2.getLineNumber());
			System.err.println("出现没有的分类！");
		}

	}

	/**
	 * 计算CHI值
	 */
	public void computeCHI() {
		CHI = (double) sampleNum * Math.pow((A * D - C * B), 2)
				/ ((double) (A + C) * (B + D) * (A + B) * (C + D)); // 防溢出，先转化为double型
		// System.out.println("Feature.computeCHI()"+CHI);
	}

	/**
	 * 计算信息增益(Information Gain，IG）
	 * 
	 * @return
	 */
	public void computeIG() {
		double p_c1 = (double) sampleNegNum / sampleNum; // 样本负类概率
		double p_c2 = (double) samplePosNum / sampleNum; // 样本正类概率
		double p_t = (double) (A + B) / sampleNum; // 包含t的文档概率
		double p_non_t = (double) (C + D) / sampleNum; // 不包含t的文档概率
		double p_c1_t = (double) B / (A + B); // 出现t时属于c1的概率
		double p_c2_t = (double) A / (A + B); // 出现t时属于c2的概率
		double p_c1_non_t = (double) D / (C + D); // 不出现t时属于c1的概率
		double p_c2_non_t = (double) C / (C + D); // 不出现t时属于c2的概率

		// 防止为0
		if (p_c1_t == 0)
			p_c1_t = 1.0 / (A + B);
		if (p_c2_t == 0)
			p_c2_t = 1.0 / (A + B);
		if (p_c1_non_t == 0)
			p_c1_non_t = 1.0 / (C + D);
		if (p_c2_non_t == 0)
			p_c2_non_t = 1.0 / (C + D);

		IG = 0;
		IG += -(p_c1 * Math.log(p_c1) + p_c2 * Math.log(p_c2)); // 第一部分
		IG += p_t * (p_c1_t * Math.log(p_c1_t) + p_c2_t * Math.log(p_c2_t)); // 第二部分
		IG += p_non_t
				* (p_c1_non_t * Math.log(p_c1_non_t) + p_c2_non_t
						* Math.log(p_c2_non_t)); // 第三部分
	}

	/**
	 * 计算互信息(Mutual Information, MI)
	 */
	public void computeMI() {
	/*	double MI_pos = Math.log((double) (A * sampleNum) / ((A + C) * (A + B))); // 正类MI值
		double MI_neg = Math.log((double) (B * sampleNum) / ((B + D) * (A + B))); // 负类MI值
		double p_c1 = (double) sampleNegNum / sampleNum; // 样本负类概率
		double p_c2 = (double) samplePosNum / sampleNum; // 样本正类概率
		MI = Math.max(p_c2 * MI_pos, p_c1 * MI_neg);*/

		double p_t = (double) (A + B) / sampleNum; 			// t的概率
		double p_c1 = (double) sampleNegNum / sampleNum; 	// 样本负类概率
		double p_c2 = (double) samplePosNum / sampleNum; 	// 样本正类概率
		double p_t_c1 = (double) A / sampleNum;				// t与c1的交
		double p_t_c2 = (double) B / sampleNum;				// t与c2的交
		MI = p_t_c1 * Math.log(p_t_c1 / (p_t * p_c1)) + p_t_c2 * Math.log(p_t_c2 / (p_t * p_c2));
	}

	public double getCHI() {
		return CHI;
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public int getA() {
		return A;
	}
	public void setA(int a) {
		A = a;
	}
	public int getB() {
		return B;
	}
	public void setB(int b) {
		B = b;
	}
	public int getC() {
		return C;
	}
	public void setC(int c) {
		C = c;
	}
	public int getD() {
		return D;
	}
	public void setD(int d) {
		D = d;
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
	public int getID() {
		return ID;
	}
	public void setID(int iD) {
		ID = iD;
	}
	public double getIG() {
		return IG;
	}
	public void setIG(double iG) {
		IG = iG;
	}
	public double getMI() {
		return MI;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	public int getDf_neg() {
		return df_neg;
	}
	public void setDf_neg(int df_neg) {
		this.df_neg = df_neg;
	}
	public int getDf_pos() {
		return df_pos;
	}
	public void setDf_pos(int df_pos) {
		this.df_pos = df_pos;
	}
	public int getAssignmentType() {
		return assignmentType;
	}
	public void setAssignmentType(int assignmentType) {
		this.assignmentType = assignmentType;
	}	

}
