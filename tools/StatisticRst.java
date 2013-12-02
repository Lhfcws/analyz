package com.yeezhao.analyz.tools;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * 统计实验结果，将svm产生的predict文件和test文件比较
 * 
 * @author congzicun
 * 
 */
public class StatisticRst {
	String[] labelStrings = { "", "投诉", "咨询", "其他" };
	Map<String, Integer> lblIndex = new HashMap<String, Integer>();
	Scanner predictScanner = null;
	Scanner dataScanner = null;
	FileWriter fw = null;

	public StatisticRst(String predictMrtxFile, String testFile,
			String outputFile) {
		try {
			predictScanner = new Scanner(new FileReader(new File(
					predictMrtxFile)));
			dataScanner = new Scanner(new FileReader(new File(testFile)));
			fw = new FileWriter(outputFile);
			lblIndex.put("投诉", 1);
			lblIndex.put("咨询", 2);
			lblIndex.put("其他", 3);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void compare() {
		try {
			double[] correct = new double[4];
			double[] precition = new double[4];
			double[] recall = new double[4];

			while (predictScanner.hasNextLine()) {
				String[] lineElments = dataScanner.nextLine().trim()
						.split("\t");
				String lbl = lineElments[1];
				String wb = lineElments[0];

				int pr = Integer.parseInt(predictScanner.nextLine().trim()
						.split("\t")[0]);
				int rst = lblIndex.get(lbl);
				precition[pr]++;
				recall[rst]++;
				if (pr == rst)
					correct[pr]++;
				else
					fw.write("Predict:" + labelStrings[pr] + "\tTrue:"
							+ labelStrings[rst] + wb + "\n");
			}
			for (int i = 1; i < 4; i++) {
				System.out.println(labelStrings[i] + " [precition]: "
						+ correct[i] / precition[i]);
				System.out.println(labelStrings[i] + " [recall]: " + correct[i]
						/ recall[i]);
			}
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		StatisticRst statisticer = new StatisticRst("test_matrix.txt.predict",
				"test_data.txt", "wrong_lbl.txt");
		statisticer.compare();
	}
}
