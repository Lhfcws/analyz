package com.yeezhao.analyz.tweet;

/**
 * @author congzicun/arber
 * 
 */

import com.yeezhao.analyz.tweet.TweetConsts.LabelType;

public abstract class AbstractTwLabeller {
	
	protected LabelType label;

	public AbstractTwLabeller( LabelType labelType ){
		this.label = labelType;
	}
	
	public LabelType getLabel(){ return label; }
	
	public abstract double computeLabelWeight(TweetObj twObj);
	
	public abstract void setModel(String mdlString);
	
	
	
}
