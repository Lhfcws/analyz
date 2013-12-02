package com.yeezhao.analyz.tweet;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yeezhao.analyz.tweet.TweetConsts.LabelType;
import com.yeezhao.commons.classifier.base.Classifier;
import com.yeezhao.commons.classifier.base.Document;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ILineParser;
import com.yeezhao.commons.util.StringUtil;

/**
 * Tweet model manager
 * @author Arber
 */
public class TweetMdlMgr implements ILineParser, Classifier {
	private static Log LOG = LogFactory.getLog(TweetMdlMgr.class);
	private static final String MODEL_BLOCK = "@MODEL";

	private boolean modelLoadMode = false;
	private String recvNickname = null; //收消息人的nickname，用于过滤自己发送的消息
	private Map<String, AbstractTwLabeller> lblMdlMap // <label, model>
		= new HashMap<String, AbstractTwLabeller>();

	/**
	 * @param mdlInputStream the model input stream
	 * @throws IOException
	 */
	public TweetMdlMgr(InputStream mdlInputStream) throws IOException {
		AdvFile.loadFileInDelimitLine(mdlInputStream, this);
	}

	public void setRecvNickname(String nickname) {
		this.recvNickname = nickname;
	}

	public String classify(Document doc) {
		return doc.containsField(TweetConsts.ATT_TW_TXT)?
				classify( doc.get(TweetConsts.ATT_TW_TXT), 
						doc.get(TweetConsts.ATT_TW_NICKNAME) ):null;
	}
	
	/**
	 * Label weibo text
	 * @param twText tweet text
	 * @param twSenderNickname tweet sender's nickname 
	 * @return the label string; or null if no label assigned
	 */	
	public String classify(String twText, String twSenderNickname){
		// 用户自己发的微博归类为null
		if( twSenderNickname != null && recvNickname != null && twSenderNickname.equals(recvNickname) )
			return null;

		TweetObj twObj = new TweetObj( twText, recvNickname);
		
		double maxWeight = -1;
		String finalLabel = null;
		for (AbstractTwLabeller labeller : lblMdlMap.values()) {
			LabelType labelType = labeller.getLabel();
			double weight = labeller.computeLabelWeight(twObj);
			if (maxWeight < weight) {
				maxWeight = weight;
				finalLabel = labelType.getLabel();
			}
		}
		if (maxWeight <= 0)
			return null;

		return finalLabel;
	}

	@Override
	public void parseLine(String line) {
		if (line.startsWith(MODEL_BLOCK)) {
			modelLoadMode = true;
			return;
		}
		if (!modelLoadMode) { // label session: label|data|modelType
			String[] strAry = line.split(StringUtil.STR_DELIMIT_1ST);
			if( strAry.length != 3 ){
				LOG.warn("Invalid model line ignored: " + line);
				return;
			}
				
			String labelString = strAry[0];
			String modelType = strAry[2];			
			if (modelType.equals(LabelType.QST.toString()))
				lblMdlMap.put(labelString, new TwQuestionLabeller());
			else if (modelType.equals(LabelType.CMP.toString()))
				lblMdlMap.put(labelString, new TwComplainLabeller());
			else{
				LabelType labelType = LabelType.fromLabel(labelString);
				if( labelType == null ){
					LOG.warn("Invalid model line ignored: " + line);
					return;					
				}
				lblMdlMap.put(labelString, new TwGeneralKwLabeller(labelType));
			}
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
}
