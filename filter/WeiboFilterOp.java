package com.yeezhao.analyz.filter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.commons.classifier.base.Classifier;
import com.yeezhao.commons.classifier.base.DocProcessor;
import com.yeezhao.commons.classifier.base.Document;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ILineParser;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;

public class WeiboFilterOp implements Classifier, DocProcessor, ILineParser  {
	public static enum WB_TYPE {
		NORMAL(0), 	//正常微薄
		AD(1), 		//广告微薄
		EMPTY(2);	//空微薄
		private final int code; 
		private WB_TYPE( int v ){ this.code = v; }
		public int getCode(){ return code; }
	}
	
	private static final int ADV_LOWER_BOUND = 3;
	
	private int maxMarkLen = 0;
	private int minMarkLen = 1000;
	private Map<String, Integer> adMarks = new HashMap<String, Integer>();
		
	public WeiboFilterOp(Configuration conf) throws IOException{
		AdvFile.loadFileInDelimitLine(
				conf.getConfResourceAsInputStream(conf.get( AppConsts.FILE_AD_FILTER )), 
				this);		
	}
	
	/**
	 * Classify a weibo text into different weibo types
	 * @param text
	 * @return the weibo type 
	 */
	public WB_TYPE classify( String text  ){
		if(text == null || text.isEmpty()) return WB_TYPE.EMPTY;
		
		List<Pair<Integer, Integer>> posPairList = 
				StringUtil.backwardMaxMatch(text, adMarks, maxMarkLen, minMarkLen);
		
		int adWeights = 0;
		if(!posPairList.isEmpty())
			for(Pair<Integer, Integer> pos : posPairList){
				String adv = text.substring(pos.first, pos.second);
				adWeights += adMarks.get(adv);
			}
		return adWeights >= ADV_LOWER_BOUND ? WB_TYPE.AD: WB_TYPE.NORMAL;		
	}
	
	@Override
	public Document process(Document doc) throws Exception {
		String result = classify( doc );
		if( result != null )
			doc.putField(AppConsts.COL_USR_FILTERTYPE, result );
		return doc;
	}

	@Override
	public String classify(Document doc) {
		return doc.containsField(AppConsts.COL_FP_TXT)? 
				Integer.toString(classify( doc.get(AppConsts.COL_FP_TXT) ).getCode()): null; 			
	}

	@Override
	public void parseLine(String line) {
		String[] segs = line.split(StringUtil.STR_DELIMIT_1ST);
		adMarks.put(segs[0], segs.length > 1 ? Integer.parseInt(segs[1]):1 );
		if (segs[0].length() > maxMarkLen)
			maxMarkLen = segs[0].length();
		if (segs[0].length() < minMarkLen)
			minMarkLen = segs[0].length();
	}
}
