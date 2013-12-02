package com.yeezhao.analyz.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

import com.mysql.jdbc.StringUtils;
import com.yeezhao.analyz.tweet.TweetConsts.LabelType;
import com.yeezhao.analyz.tweet.TweetObj;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ILineParser;

/**
 * 对微博进行分词，将分词的统计结果写入term_tbl.txt
 * 去掉了长度为1的和包含数字的
 * 
 * @author congzicun
 * 
 */
public class ParseTweet implements ILineParser {

	private Map<String, Integer[]> termTbl = new HashMap<String, Integer[]>();
	FileWriter fw = null;

	public static void main(String[] args) {
		try {
			ParseTweet parseTw = new ParseTweet(new FileInputStream(new File(
					"data/train_data.txt")));
			parseTw.SaveTbl2File();
			System.out.println("Finish");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ParseTweet(InputStream in) {
		try {
			AdvFile.loadFileInDelimitLine(in, this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void parseLine(String line) {
		String label = line.split("\t")[1];
		String wbText = line.split("\t")[0];
		TweetObj twObj = new TweetObj(wbText, "null");

		if (StringUtils.isEmptyOrWhitespaceOnly(twObj.getTwText()))
			return;

		List<Term> terms = ToAnalysis.paser(twObj.getTwText());
		for (Term term : terms) {
			addTerm2Tbl(term.getName(), label);
		}

		for (String emotion : twObj.getEmotion()) {
			addTerm2Tbl(emotion, label);
		}
	}

	private void addTerm2Tbl(String term, String label) {
		term = term.toLowerCase();

		if (term.length() < 2)
			return;
		if (term.matches(".*[0-9].*"))
			return;
		if (term.matches(".*[a-z]+,*"))
			return;

		if (!termTbl.containsKey(term)) {
			Integer[] typeValues = new Integer[4];
			Arrays.fill(typeValues, 0);
			termTbl.put(term, typeValues);
		}
		LabelType labelType = LabelType.fromLabel(label);
		switch (labelType) {
		case CMP:
			termTbl.get(term)[0] += 1;
			break;
		case QST:
			termTbl.get(term)[1] += 1;
			break;
		default:
			termTbl.get(term)[2] += 1;
			break;
		}
		termTbl.get(term)[3] += 1;
	}

	public void SaveTbl2File() {
		try {
			fw = new FileWriter("term_tbl.txt");
			fw.write("Term\tCMP\tQST\tRPS\tTOTAL\n");
			String lineTmpl = "%s\t%d\t%d\t%d\t%d\n";
			for (Entry<String, Integer[]> entry : termTbl.entrySet()) {
				Integer[] number = entry.getValue();
				fw.write(String.format(lineTmpl, entry.getKey(), number[0],
						number[1], number[2], number[3]));
			}
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
