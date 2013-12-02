package com.yeezhao.analyz.age;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.yeezhao.analyz.age.classifier.AgeClassifier;
import com.yeezhao.analyz.group.FootPrintWritable;
import com.yeezhao.analyz.group.UserFootPrint;
import com.yeezhao.analyz.util.AppConsts;

public class AgeLblReducer extends
		Reducer<Text, FootPrintWritable, NullWritable, NullWritable> {

	private static Log LOG = LogFactory.getLog(AgeLblReducer.class);

	private AgeClassifier ageClassifier;	// 年龄分类器
	private List<Put> puts = new LinkedList<Put>();
	private static final byte[] COL_FAL_ANALYZ = Bytes.toBytes("analyz");
	private static final byte[] COL_AGE = Bytes.toBytes("age");
	private HTable userInfoTbl;

	private static enum REDUCE_COUNT {
		NUM_ALL_USER, NUM_AGE_USER, NUM_AGE_USER0, NUM_AGE_USER1, NUM_AGE_USER2, NUM_AGE_USER3, NUM_AGE_USER4
	}

	public void setup(Context context) {
		try {
			Configuration conf = context.getConfiguration();
			String path = conf.getResource(conf.get("age.dir")).toString();
			path = path.substring(path.indexOf(":") + 1) + File.separatorChar;

			String dataFileName = path + conf.get("age.NB.data.file");
			String modelFileName = path + conf.get("age.NB.model.file");
			String attriWordFileName = path + conf.get("age.attri.word.file");
			ageClassifier = new AgeClassifier(dataFileName, modelFileName,
					attriWordFileName);
			
			userInfoTbl = new HTable(conf, conf.get(AppConsts.HTBL_USER_PROFILE));
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e);
		}
	}

	public void reduce(Text key, Iterable<FootPrintWritable> values,
			Context context) throws IOException {
		LOG.info("key: " + key);
		String age = getAge(values, context);
		Put put = new Put(key.getBytes());
		put.add(COL_FAL_ANALYZ, COL_AGE, Bytes.toBytes(age));
		puts.add(put);

		updateCounter(age, context);
		
		if (puts.size() >= 1000) {
			userInfoTbl.put(puts);
			userInfoTbl.flushCommits();
			puts.clear();
		}
	}

	/**
	 * 获取分类后的年龄标签
	 * 
	 * @param values
	 * @return
	 */
	private String getAge(Iterable<FootPrintWritable> values, Context context) {
		Iterator<FootPrintWritable> itor = values.iterator();
		List<String> textList = new ArrayList<String>();
		List<String> tagList = new ArrayList<String>();
		while (itor.hasNext()) {
			UserFootPrint fp = new UserFootPrint(itor.next());
			// LOG.info(itor.next());
			if (fp.getDataType().equals(AppConsts.FP_MSG)) {
				textList.add(fp.getContent());
			} else if (fp.getDataType().equals(AppConsts.FP_TAG)) {
				tagList.addAll(Arrays.asList(fp.getContent().split("[|]")));
			} else {
				LOG.error("error type: " + fp.getContent());
			}
		}
		// LOG.info("result: " +
		// ageClassifier.classify(AgeAdapter.getUser(textList, tagList)));

		context.getCounter(REDUCE_COUNT.NUM_ALL_USER).increment(1);
		return ageClassifier.classify(AgeAdapter.getUser(textList, tagList));
	}

	/**
	 * 
	 * @param key
	 * @param age
	 */
	private void updateCounter(String age, Context context) {
		if (age != null && age.length() > 0) {
			try {
				switch (Integer.valueOf(age).intValue()) {
				case 1:
					context.getCounter(REDUCE_COUNT.NUM_AGE_USER1).increment(1);
					break;
				case 2:
					context.getCounter(REDUCE_COUNT.NUM_AGE_USER2).increment(1);
					break;
				case 3:
					context.getCounter(REDUCE_COUNT.NUM_AGE_USER3).increment(1);
					break;
				case 4:
					context.getCounter(REDUCE_COUNT.NUM_AGE_USER4).increment(1);
					break;
				default:
					context.getCounter(REDUCE_COUNT.NUM_AGE_USER0).increment(1);
					break;
				}
				context.getCounter(REDUCE_COUNT.NUM_AGE_USER).increment(1);
			} catch (NumberFormatException e) {
				e.printStackTrace();
				LOG.error(e);
			}
		}
	}

	public void cleanup(Context context) {
		if (userInfoTbl != null) {
			try {
				if (puts.size() > 0) {
					userInfoTbl.put(puts);
					userInfoTbl.flushCommits();
					puts.clear();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally{
				try {
					userInfoTbl.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
	}

}