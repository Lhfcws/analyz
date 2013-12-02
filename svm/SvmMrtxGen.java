package com.yeezhao.analyz.svm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;

import com.yeezhao.analyz.tweet.TweetObj;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ILineParser;

/**
 * 将输入的数据集合转换为特征矩阵
 * @author congzicun
 *
 */
public class SvmMrtxGen implements ILineParser {

	private SvmVecGen svmVecGen = null;
	private FileWriter fw = null;

	public static void main(String[] args) {
		try {
			SvmMrtxGen svmMrtxGen = new SvmMrtxGen(new FileInputStream(
					new File("data/total_data.txt")),"test_matrix.txt");
			svmMrtxGen.close();
			svmMrtxGen = new SvmMrtxGen(new FileInputStream(
					new File("data/train_data.txt")),"train_matrix.txt");
			svmMrtxGen.close();
			System.out.println("Finish Gen Matrix");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public SvmMrtxGen(InputStream in,String outputname) {
		try {
			String mdlPaht = "src/main/resources/tweet-svm-mdl.txt";
			svmVecGen = new SvmVecGen(new FileInputStream(new File(mdlPaht)));
			fw = new FileWriter(outputname);
			AdvFile.loadFileInDelimitLine(in, this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void parseLine(String line) {
		String[] lineElments = line.split("\t");
		String lbl = "3";
		if (lineElments.length == 2) {
			if ("投诉".equals(lineElments[1])) {
				lbl = "1";
			} else if ("咨询".equals(lineElments[1])) {
				lbl = "2";
			}
		}
		TweetObj twObj = new TweetObj(lineElments[0], "null");
		Double[] featureVec = svmVecGen.genVec(twObj);
		String featureStr = genDelimitedString(featureVec);
		try {
			fw.write(lbl + "\t" + featureStr + "\n");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String genDelimitedString(Double[] featureVec) {

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < featureVec.length; i++) {
			if (featureVec[i] != 0)
				sb.append(i).append(":").append(featureVec[i]).append("\t");
		}
		return sb.substring(0, sb.length() - 1);
	}

}
