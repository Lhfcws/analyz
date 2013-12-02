package com.yeezhao.analyz.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

import com.yeezhao.analyz.tweet.TweetObj;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ILineParser;
import com.yeezhao.commons.util.ValueComparator;

public class TFIDF implements ILineParser {
	private Map<String, Double[]> TF = new HashMap<String, Double[]>();
	private Map<String, Integer> lblIndex = new HashMap<String, Integer>();
	private double[] counter = new double[4];

	public static void main(String[] args) {
		try {
			TFIDF tfidf = new TFIDF(new FileInputStream(new File(
					"data/total_data.txt")));
			for (int i = 1; i < 4; i++)
				tfidf.showRst(i);
			System.out.println("Finish");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public TFIDF(InputStream in) {
		try {
			lblIndex.put("投诉", 1);
			lblIndex.put("咨询", 2);
			lblIndex.put("其他", 3);
			AdvFile.loadFileInDelimitLine(in, this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void parseLine(String line) {

		String[] lineElements = line.toLowerCase().trim().split("\t");
		String label = lineElements[1];
		String wbText = lineElements[0];
		TweetObj twObj = new TweetObj(wbText, "null");
		List<Term> terms = ToAnalysis.paser(twObj.getTwText());

		for (Term term : terms) {
			if (!TF.containsKey(term.getName())) {
				Double[] tfDoubles = new Double[4];
				Arrays.fill(tfDoubles, 0.0);
				TF.put(term.getName(), tfDoubles);
			}
			TF.get(term.getName())[lblIndex.get(label)]++;
		}
		for (String expression : twObj.getEmotion()) {
			if (!TF.containsKey(expression)) {
				Double[] tfDoubles = new Double[4];
				Arrays.fill(tfDoubles, 0.0);
				TF.put(expression, tfDoubles);
			}
			TF.get(expression)[lblIndex.get(label)]++;
		}
		counter[lblIndex.get(label)]++;
		counter[0]++;
	}

	public void showRst(int lblType) {
		try {
			FileWriter fw = new FileWriter(Integer.toString(lblType));
			Map<String, Double> TFIDF = new HashMap<String, Double>();
			ValueComparator<String, Double> vlc = new ValueComparator<String, Double>(
					TFIDF);
			TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(
					vlc);

			for (Entry<String, Double[]> entry : TF.entrySet()) {
				TFIDF.put(
						entry.getKey(),
						entry.getValue()[lblType]
								* Math.log(counter[0] / counter[lblType]));
			}
			sorted_map.putAll(TFIDF);
			for (String term : sorted_map.keySet()) {
				if (!ifDropTerm(term) && sorted_map.get(term) != 0) {
					fw.write(term + "\t" + sorted_map.get(term) + "\n");
				}
			}
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
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
		if (term.matches("[一二三四五六七八九十]+"))
			return true;
		if (term.length() < 2)
			return true;
		return false;
	}
}
