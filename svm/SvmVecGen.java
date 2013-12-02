package com.yeezhao.analyz.svm;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

import com.yeezhao.analyz.tweet.TweetObj;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ILineParser;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;

public class SvmVecGen implements ILineParser {
	// map feature to index of the feature vector
	private Map<String, Integer> featureMap = new HashMap<String, Integer>();
	private List<MdlType> featureTypeList = new LinkedList<MdlType>();
	private Set<String> etermTbl = new HashSet<String>();
	private List<Pair<Integer, Integer>> lenTbl = new LinkedList<Pair<Integer, Integer>>();
	private int featureIndex = 0;
	private boolean mdlStart = false;

	public SvmVecGen(InputStream in) {
		try {
			AdvFile.loadFileInDelimitLine(in, this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void parseLine(String line) {
		if (line.indexOf("#") == 0)
			return;
		if (line.equals("@model")) {
			mdlStart = true;
			return;
		}
		line = line.trim().toLowerCase();

		if (!mdlStart) {
			featureTypeList.add(MdlType.fromType(line));
		} else {
			String[] lineElements = line.split(StringUtil.STR_DELIMIT_1ST);
			String featureline = lineElements[1];
			String mdlTypeString = lineElements[0];
			MdlType mdlType = MdlType.fromType(mdlTypeString);
			if (mdlType == null) {
				System.err.println("Model Type Not Supported");
				return;
			}
			addFeatures(featureline, mdlType);
		}

	}

	/**
	 * 将每类feature中具体的内容映射到index
	 * 
	 * @param featureline
	 * @param mdlType
	 */
	private void addFeatures(String featureline, MdlType mdlType) {
		String[] features = null;
		switch (mdlType) {
		case TM:
			features = featureline.split(StringUtil.STR_DELIMIT_2ND);
			for (String term : features) {
				if (!featureMap.containsKey(term))
					featureMap.put(term, featureIndex++);
			}
			break;
		case SMBL:
			features = featureline.split(StringUtil.STR_DELIMIT_2ND);
			for (String symbol : features) {
				if (!featureMap.containsKey(symbol))
					featureMap.put(symbol, featureIndex++);
			}
			break;
		case SUBNUM:
			features = featureline.split(StringUtil.STR_DELIMIT_2ND);
			int spanIndex = 0;
			for (String feature : features) {
				featureMap.put(Integer.toString(spanIndex), featureIndex++);

				String[] lenElements = feature.split("-");
				lenTbl.add(new Pair<Integer, Integer>(Integer
						.parseInt(lenElements[0]), Integer
						.parseInt(lenElements[1])));
				spanIndex++;
			}
			break;
		case ETERM:
			features = featureline.split(StringUtil.STR_DELIMIT_2ND);
			for (String symbol : features) {
				featureMap.put(symbol, featureIndex++);
				etermTbl.add(symbol);
			}
			break;
		case EXPRSSN:
			features = featureline.split(StringUtil.STR_DELIMIT_2ND);
			for (String expression : features) {
				if (!featureMap.containsKey(expression))
					featureMap.put(expression, featureIndex++);
			}
			break;
		default:
			break;
		}
	}

	public Double[] genVec(TweetObj twoObj) {
		Double[] featureVec = new Double[featureMap.size() + 1];
		Arrays.fill(featureVec, 0.0);
		List<Term> terms = ToAnalysis.paser(twoObj.getTwText());
		for (MdlType mdlType : featureTypeList) {
			switch (mdlType) {
			case TM:
				featureVec = processTermFeature(featureVec, terms);
				break;
			case SMBL:
				// TODO 这里要想办法和term feature分开
				// featureVec = processTermFeature(featureVec, terms);
				break;
			case ETERM:
				featureVec = processEmTermFeature(featureVec,
						twoObj.getSubSentences());
				break;
			case SUBNUM:
				featureVec = processSubNumFeature(featureVec,
						twoObj.getSubSentences().length);
				break;
			case EXPRSSN:
				featureVec = processExpressionFeature(featureVec,
						twoObj.getEmotion());
				break;
			default:
				break;
			}
		}
		return featureVec;
	}

	/**
	 * 单字，数字和英文会被丢弃
	 * 
	 * @param term
	 * @return
	 */
	private boolean ifDropTerm(String term) {
		if (term.matches(".*[0-9]+.*"))
			return true;
		if (term.matches(".*[a-z]+.*"))
			return true;
		return false;
	}

	/**
	 * 处理表情特征
	 * 
	 * @param featureVec
	 * @param emotion
	 * @return
	 */
	private Double[] processExpressionFeature(Double[] featureVec,
			List<String> emotion) {
		for (String expression : emotion) {
			if (featureMap.containsKey(expression)) {
				featureVec[featureMap.get(expression)] = 1.0;
			}
		}
		return featureVec;
	}

	/**
	 * 微博中句子的数量所落在的区间
	 * 
	 * @param featureVec
	 * @param len
	 * @return
	 */
	private Double[] processSubNumFeature(Double[] featureVec, int len) {
		int spanIndex = 0;
		for (Pair<Integer, Integer> span : lenTbl) {
			if (len >= span.getFirst() && len <= span.getSecond()) {
				featureVec[featureMap.get(Integer.toString(spanIndex))] = 1.0;
			}
			spanIndex++;
		}
		return featureVec;
	}

	/**
	 * 每个子句子是否以特殊的语气字符结尾
	 * 
	 * @param featureVec
	 * @param subSentences
	 * @return
	 */
	private Double[] processEmTermFeature(Double[] featureVec,
			String[] subSentences) {
		for (String sentence : subSentences) {
			int validPos = (sentence.length() / 2);
			for (String symbol : etermTbl)
				if (sentence.indexOf(symbol) >= validPos) {
					featureVec[featureMap.get(symbol)] = 1.0;
				}
		}
		return featureVec;
	}

	/**
	 * 处理关键字feature和符号feature，数字和单词会被丢弃,现在的问题是符号也会被识别出来
	 * 
	 * @param featureVec
	 * @param terms
	 * @return
	 */
	private Double[] processTermFeature(Double[] featureVec, List<Term> terms) {
		for (Term term : terms) {
			if (ifDropTerm(term.getName()))
				continue;
			if (featureMap.containsKey(term.getName())
					&& !etermTbl.contains(term.getName())) {
				featureVec[featureMap.get(term.getName())] += 1.0;
			}
		}
		return featureVec;
	}

	/**
	 * 模型的类别
	 * 
	 * @author congzicun
	 * 
	 */
	enum MdlType {
		TM("term"), SMBL("symbol"), SUBNUM("subsequent_num"), ETERM(
				"emotion_term"), EXPRSSN("expression");
		private String type = null;

		private MdlType(String type) {
			this.type = type;
		}

		public String getType() {
			return this.type;
		}

		public static MdlType fromType(String mdlType) {
			if (mdlType.equals(TM.getType()))
				return TM;
			else if (mdlType.equals(SMBL.getType()))
				return SMBL;
			else if (mdlType.equals(SUBNUM.getType()))
				return SUBNUM;
			else if (mdlType.equals(ETERM.getType()))
				return ETERM;
			else if (mdlType.equals(EXPRSSN.getType()))
				return EXPRSSN;
			else
				return null;
		}
	}

}
