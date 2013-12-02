package com.yeezhao.analyz.group;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.yeezhao.analyz.group.UserGroup.GroupType;
import com.yeezhao.analyz.group.UserGroup.SrcType;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.commons.classifier.base.Classifier;
import com.yeezhao.commons.classifier.base.DocProcessor;
import com.yeezhao.commons.classifier.base.Document;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ILineParser;
import com.yeezhao.commons.util.StringUtil;

/**
 * Fans group model manager. Different from other label, this type of model
 * utilize the information in user.profile table.
 * 
 * @author Arber
 * 
 */
public class FansGroupMdlMgr implements ILineParser, Classifier, DocProcessor {
	// Specify valid src/group type processed in this labeller
	private static EnumSet<SrcType> VALID_SRCTYPE = EnumSet.of(SrcType.FOLLOW);
	private static EnumSet<GroupType> VALID_GRPTYPE = EnumSet
			.of(GroupType.FANS);

	private boolean modelLoadMode = false;
	private Map<String, BaseGroupLabeller> grpMdlMap // <group, model>
	= new HashMap<String, BaseGroupLabeller>();

	/**
	 * @param mdlInputStream
	 *            the model input stream
	 * @throws IOException
	 */
	public FansGroupMdlMgr(InputStream mdlInputStream) throws IOException {
		AdvFile.loadFileInDelimitLine(mdlInputStream, this);
	}

	public FansGroupMdlMgr(Configuration conf) throws IOException {
		String modelFile = conf.get(AppConsts.GROUP_MODEL_FILE);
		AdvFile.loadFileInDelimitLine(
				conf.getConfResourceAsInputStream(modelFile), this);
	}

	/**
	 * Label the document.
	 * 
	 * @return the group label string; or null if no label assigned
	 */
	public String classify(Document doc) {
		StringBuilder sb = new StringBuilder();
		for (BaseGroupLabeller labeller : grpMdlMap.values()) {
			String label = labeller.classify(doc);
			if (label != null)
				sb.append(label).append(StringUtil.DELIMIT_1ST);
		}
		return sb.length() > 0 ? sb.substring(0, sb.length() - 1) : null;
	}

	@Override
	public Document process(Document doc) throws Exception {
		String label = classify(doc);
		if (label != null) {
			doc.putField(BaseGroupLabeller.LABEL, label);
		}
		return doc;
	}

	@Override
	public void parseLine(String line) {
		if (line.startsWith(BaseGroupLabeller.MODEL_BLOCK)) {
			modelLoadMode = true;
			return;
		}
		if (!modelLoadMode) { // label session: <group>|<data>|<model type>
			String[] strAry = line.split(StringUtil.STR_DELIMIT_1ST);
			if (strAry.length != 3) {
				System.err.println("Invalid line: " + line);
				return;
			}
			SrcType srcType = SrcType.valueOf(strAry[1]);
			GroupType groupType = GroupType.valueOf(strAry[2]);

			if (VALID_SRCTYPE.contains(srcType)
					&& VALID_GRPTYPE.contains(groupType)) {
				if (groupType.equals(GroupType.FANS))
					grpMdlMap.put(strAry[0], new FansGroupLabelOp(strAry[0]));
			}
		} else { // model session: <group name>|<model string>
			String upLine = line.toUpperCase();
			int pos = line.indexOf(StringUtil.DELIMIT_1ST);
			if( pos == -1 || pos == line.length() - 1 ){
				System.err.println("Invalid model line ignored: " + line);
				return;
			}
			String groupName = upLine.substring(0, pos);
			if (grpMdlMap.containsKey(groupName)) {
				BaseGroupLabeller labeller = grpMdlMap.get(groupName);
				labeller.setModel( upLine.substring(pos+1) );
			} 
		}
	}
}
