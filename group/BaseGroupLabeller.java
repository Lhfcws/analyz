package com.yeezhao.analyz.group;

import com.yeezhao.commons.classifier.base.Classifier;
import com.yeezhao.commons.classifier.base.DocProcessor;
import com.yeezhao.commons.classifier.base.Document;

/**
 * Base group labeller
 * @author Arber
 */
public abstract class BaseGroupLabeller implements Classifier, DocProcessor {
	public static final String MODEL_BLOCK = "@MODEL";
	//Output attributes
	public static final String LABEL = "label";
	
	protected String label;
	
	public abstract void setModel(String mdlString );
	
	@Override
    public Document process(Document doc) throws Exception{
		String label = classify(doc);
		if (label != null)
			doc.putField( LABEL, label);
		return doc;
	}


	
}
