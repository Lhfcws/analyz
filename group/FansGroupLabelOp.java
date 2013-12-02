package com.yeezhao.analyz.group;

import java.util.HashMap;
import java.util.Map;

import com.yeezhao.commons.classifier.base.Document;
import com.yeezhao.commons.util.StringUtil;

/**
 * Base operation for analyzing fans group
 * 
 * @author Arber
 */
public class FansGroupLabelOp extends BaseGroupLabeller {
	private final static int DEFAULT_MIN_FOLLOW = 2;
	private final static String COL_FMLY_FRIEND = "friend:";
	
	private Map<String, String> fglpKW = new HashMap<String, String>();
	
	private int minFollow = DEFAULT_MIN_FOLLOW;

	public FansGroupLabelOp(String label){
		this.label = label;
	}
	@Override
	public String classify(Document doc) {
		int followNum = 0;
		for (String kw : fglpKW.keySet()) 
			if( doc.containsField(kw) )
				followNum++;
		
		return followNum >= minFollow ? label: null;
	}

	/**
	 * model string format: [sn|tx]:uid[$uid]|min:<minFollow>
	 */
	@Override
	public void setModel(String mdlString) {
		
		String[] strAry = mdlString.split(StringUtil.STR_DELIMIT_1ST);
		for( String kvStr: strAry){
			String[] kvAry = kvStr.split(":");
			if( kvAry.length != 2 ){
				System.err.println("Invalid key-value string ignored: " + kvStr );
				continue;
			}
			if (kvAry[0].equals("SN") || kvAry[0].equals("TX")) {
				for (String s : kvAry[1].split(StringUtil.STR_DELIMIT_2ND)) {
					fglpKW.put(COL_FMLY_FRIEND + s, label);
				}
			}
			else if( kvAry[0].equals("MIN") ){
				minFollow = Integer.parseInt(kvAry[1]);
			}
			else {
				System.err.println("Invalid key-value string ignored: " + kvStr );
			}
		}
	}
}
