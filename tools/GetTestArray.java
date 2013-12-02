package com.yeezhao.analyz.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ILineParser;

/**
 * 将总的数据集分为训练集和测试集
 * 
 * @author congzicun
 * 
 */
public class GetTestArray implements ILineParser {
	private Map<String, LinkedList<String>> wbs = new HashMap<String, LinkedList<String>>();

	public static void main(String[] args) {
		try {
			GetTestArray getArray = new GetTestArray(new FileInputStream(
					new File("data/total_data.txt")));
			System.out.println("Finish");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public GetTestArray(InputStream in) {
		try {
			FileWriter fw = new FileWriter("test_data.txt");
			FileWriter fwtrain = new FileWriter("train_data.txt");

			wbs.put("投诉", new LinkedList<String>());
			wbs.put("咨询", new LinkedList<String>());
			wbs.put("其他", new LinkedList<String>());
			AdvFile.loadFileInDelimitLine(in, this);

			int index = 0;
			for (String wbText : wbs.get("投诉")) {
				if (index < 100)
					fw.write(wbText + "\t投诉\n");
				else {
					fwtrain.write(wbText + "\t投诉\n");
				}
				index++;
			}
			System.out.println(index);

			index = 0;
			for (String wbText : wbs.get("咨询")) {
				if (index < 100)
					fw.write(wbText + "\t咨询\n");
				else {
					fwtrain.write(wbText + "\t咨询\n");
				}
				index++;
			}
			System.out.println(index);

			index = 0;
			for (String wbText : wbs.get("其他")) {
				if (index < 100)
					fw.write(wbText + "\t其他\n");
				else {
					fwtrain.write(wbText + "\t其他\n");
				}
				if(index > 1200)
					break;
				index++;
			}
			System.out.println(index);

			fw.close();
			fwtrain.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void parseLine(String line) {
		String[] lineElments = line.trim().split("\t");
		wbs.get(lineElments[1]).add(lineElments[0]);
	}
}
