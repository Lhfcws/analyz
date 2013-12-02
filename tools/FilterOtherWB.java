package com.yeezhao.analyz.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.yeezhao.analyz.tweet.AbstractTwLabeller;
import com.yeezhao.analyz.tweet.TwComplainLabeller;
import com.yeezhao.analyz.tweet.TwGeneralKwLabeller;
import com.yeezhao.analyz.tweet.TwQuestionLabeller;
import com.yeezhao.analyz.tweet.TweetConsts;
import com.yeezhao.analyz.tweet.TweetObj;
import com.yeezhao.analyz.tweet.TweetConsts.LabelType;
import com.yeezhao.commons.classifier.base.Classifier;
import com.yeezhao.commons.classifier.base.Document;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ILineParser;
import com.yeezhao.commons.util.StringUtil;

/**
 * 用来产生训练集，将其中明显不会是投诉和咨询分出去,整体上与规则是一致的，只不过这里 只选择“其他”类别最强的那些，先分出去 目前其他的准确率为99.9%
 * 
 * @author congzicun
 * 
 */
public class FilterOtherWB implements ILineParser, Classifier {
	private static Log LOG = LogFactory.getLog(FilterOtherWB.class);
	private static final String MODEL_BLOCK = "@MODEL";
	private boolean modelLoadMode = false;
	private Map<String, AbstractTwLabeller> lblMdlMap // <label, model>
	= new HashMap<String, AbstractTwLabeller>();

	private static final String OUTPUT_FILE = "predict_pure_others_wb.txt";
	private static final String LABEL_MDL_FILE = "src/main/resources/tweet-filter-model.txt.";
//	private static final String INPUT_FILE = "data/sample_0.1_1.txt";
	 private static final String INPUT_FILE = "/tmp/a";
	private static FileWriter fw = null;

	public static void main(String[] args) {
		try {
			fw = new FileWriter(OUTPUT_FILE);
			FilterOtherWB filter = new FilterOtherWB(new FileInputStream(
					new File(LABEL_MDL_FILE)));
			Scanner scanner = new Scanner(new FileReader(new File(INPUT_FILE)));
			while (scanner.hasNextLine()) {
				String wbText = scanner.nextLine().trim().split("\t")[0];
				Document document = new Document("中国电信广东网厅");
				document.putField(TweetConsts.ATT_TW_TXT, wbText);
				fw.write(filter.classify(document) + "\n");
			}
			System.out.println("Filter Finish");
			fw.close();
			StatisticRst statisticer = new StatisticRst(OUTPUT_FILE,
					INPUT_FILE, "wrong_lbl.txt");
			statisticer.compare();
			filter.rm(INPUT_FILE, OUTPUT_FILE, "CLEAN_TRAIN_DATA.txt");
			System.out.println("All Finish");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public FilterOtherWB(InputStream in) {
		try {
			AdvFile.loadFileInDelimitLine(in, this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String classify(Document doc) {
		return doc.containsField(TweetConsts.ATT_TW_TXT) ? classify(doc
				.get(TweetConsts.ATT_TW_TXT)) : null;
	}

	/**
	 * 与规则分类器相比只有这里有所不同。如果分类器为投诉或者咨询并且分出来的权重大于0，那么就 认为一定不是其他类别的。
	 * 如果分类器为其他的，并且权重大于0，那么才认为这个是其他类别的
	 * 
	 * @param twText
	 * @param twSenderNickname
	 * @return
	 */
	public String classify(String twText) {
		try {
			TweetObj twObj = new TweetObj(twText, "null");
			double maxWeight = -1;
			for (AbstractTwLabeller labeller : lblMdlMap.values()) {
				maxWeight = labeller.computeLabelWeight(twObj);
			}
			if (maxWeight > 0) {
				return "3";
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "1";
	}

	@Override
	public void parseLine(String line) {
		if (line.startsWith(MODEL_BLOCK)) {
			modelLoadMode = true;
			return;
		}
		if (!modelLoadMode) { // label session: label|data|modelType
			String[] strAry = line.split(StringUtil.STR_DELIMIT_1ST);
			if (strAry.length != 3) {
				LOG.warn("Invalid model line ignored: " + line);
				return;
			}

			String labelString = strAry[0];
			LabelType labelType = LabelType.fromLabel(labelString);
			if (labelType == null) {
				LOG.warn("Invalid model line ignored: " + line);
				return;
			}
			lblMdlMap.put(labelString, new TwFilterLabeller(labelType));
		} else { // model session
			String[] strAry = line.toUpperCase().split(
					StringUtil.STR_DELIMIT_1ST);
			if (strAry.length < 2) {
				System.err.println("Invalid model line ignored: " + line);
				return;
			}
			if (lblMdlMap.containsKey(strAry[0])) {
				AbstractTwLabeller labeller = lblMdlMap.get(strAry[0]);
				labeller.setModel(strAry[1]);
			} else {
				System.err.println("Label missing for this model:" + strAry[0]);
			}
		}
	}

	/**
	 * 这个方法主要就是用来将刚才分出的其他微博从训练集合中去掉
	 * 
	 * 
	 */
	public void rm(String oriFile, String lblFile, String outFile) {
		try {
			Scanner oriScanenr = new Scanner(new FileReader(new File(oriFile)));
			Scanner lblScanenr = new Scanner(new FileReader(new File(lblFile)));
			FileWriter fw = new FileWriter(outFile);
			while (oriScanenr.hasNextLine()) {
				String wb = oriScanenr.nextLine().trim();
				String type = lblScanenr.nextLine().trim();
				if (type.equals("3"))
					continue;
				fw.write(wb + "\n");
			}
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
