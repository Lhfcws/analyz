package com.yeezhao.analyz.filter;

import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.commons.classifier.base.Document;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.StringUtil;

public class LocalRobotAnalyze implements CliRunner {
	HashMap<String, Info> usrInfo = new HashMap<String, Info>();
	private static final String cmdName = "lclRbtAnalyz";

	@Override
	public Options initOptions() {
		Options opts = new Options();
		opts.addOption("wb", true, "weibo file");
		opts.addOption("usr", true, "user file");
		opts.addOption(AdvCli.CLI_PARAM_O, true, "output file");
		return opts;
	}

	@Override
	public void start(CommandLine cmdLine) {
		Scanner scanner;
		String line = null;
		int q = 0;
		try {
			scanner = new Scanner(new FileReader(cmdLine.getOptionValue("wb")));

			Info info = null;
			String uid = null;

			ArrayList<Document> docs = new ArrayList<Document>();
			Document doc = null;
			while (scanner.hasNextLine()) {
				line = scanner.nextLine();
				if (line.indexOf("[ROW]") != -1) {
					if (doc != null) {
						// 对缺失的微博数据做一些修正
						fixLstFtrErr(doc);
						docs.add(doc);
					}
					doc = new Document(
							line.split(StringUtil.STR_DELIMIT_1ST)[3]);
					doc.putField(WeiboConsts.ATT_WB_USER,
							line.split(StringUtil.STR_DELIMIT_1ST)[1]);
				}
				if (line.indexOf(AppConsts.COL_FP_TXT) != -1) {
					line = scanner.nextLine();
					doc.putField(WeiboConsts.ATT_WB_TXT, line);
				} else if (line.indexOf(AppConsts.COL_FP_TIME) != -1) {
					line = scanner.nextLine();
					doc.putField(WeiboConsts.ATT_WB_PUBTIME, line);
				} else if (line.indexOf(AppConsts.COL_FP_URL) != -1) {
					line = scanner.nextLine();
					doc.putField(WeiboConsts.ATT_WB_URL, line);
				} else if (line.indexOf(AppConsts.COL_FP_SOURCE) != -1) {
					line = scanner.nextLine();
					doc.putField(WeiboConsts.ATT_WB_SOURCE, line);
				} else if (line.indexOf(AppConsts.COL_FP_FEATURE) != -1) {
					line = scanner.nextLine();
					doc.putField(WeiboConsts.ATT_WB_FEATURE, line);
				}

			}
			scanner.close();
			scanner = new Scanner(new FileReader(cmdLine.getOptionValue("usr")));
			while (scanner.hasNextLine()) {
				line = scanner.nextLine();
				if (line.indexOf("[ROW]") != -1) {
					uid = line.split(StringUtil.STR_DELIMIT_1ST)[1];
					info = new Info(uid);
					usrInfo.put(uid, info);
				}
				if (line.indexOf(AppConsts.COL_USR_FOLLOWCOUNT) != -1) {
					line = scanner.nextLine();
					info.followers_count = Integer.parseInt(line);
				} else if (line.indexOf(AppConsts.COL_USR_FRIENDCOUNT) != -1) {
					line = scanner.nextLine();
					info.friends_count = Integer.parseInt(line);
				} else if (line.indexOf(AppConsts.COL_USR_STATUSCOUNT) != -1) {
					line = scanner.nextLine();
					info.statuses_count = Integer.parseInt(line);
				}
			}

			for (Document weibo : docs) {
				q++;
				uid = weibo.get(WeiboConsts.ATT_WB_USER);
				if (!usrInfo.containsKey(uid))
					usrInfo.put(uid, new Info(uid));
				info = usrInfo.get(uid);
				String content = weibo.get(WeiboConsts.ATT_WB_TXT);
				String wbTxt = content;
				content = WeiboProcess.rmOthMsg(content);
				content = WeiboProcess.extractAt(weibo, content);
				if (content.indexOf("勋章") != -1 || wbTxt.indexOf("通过@微盘") != -1
						|| wbTxt.indexOf("回复@") != -1
						|| content.indexOf("徽章") != -1)
					continue;
				info.amount++;
				// 统计时间信息
				statisticTm(info,
						Integer.parseInt(weibo.get(WeiboConsts.ATT_WB_PUBTIME)));
				// 统计转发和url信息
				if (statisticUrl(info, content))
					info.urltweets++;
				else {
					statisticRps(info, weibo.get(WeiboConsts.ATT_WB_FEATURE));
				}
				// 统计@信息
				statisticAt(info, doc);
			}

			// 输出结果
			FileWriter fw = new FileWriter(
					cmdLine.getOptionValue(AdvCli.CLI_PARAM_O));
			for (Info i : usrInfo.values()) {
				int evidence = 0;
				int len = 0;
				int ori = 0;
				for (int j = 0; j < i.period.size(); j++) {
					if (j == 0) {
						ori = i.period.get(j);
						continue;
					}
					if (Math.abs(i.period.get(j) - i.period.get(j - 1)) < 60
							&& i.period.get(j) - ori < 120)
						len++;
					else {
						if (len > 1)
							evidence += (len + 1);
						len = 0;
						ori = i.period.get(j);
					}
				}

				if (evidence * 2 > i.amount)
					fw.write(i.uid + "\n");
				else if (i.followers_count <= 5 || i.friends_count <= 5
						|| i.statuses_count <= 5) {
					fw.write(i.uid + "\n");
				} else if (i.urltweets + i.rps >= i.amount)
					fw.write(i.uid + "\n");
				else if (i.atusr.size() > 20
						&& (double) i.atusertweets / (double) i.amount > 0.8) {
					fw.write(i.uid + "\n");
				}
			}
			fw.close();

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(line);
			System.out.println(q);
		}

	}

	/**
	 * 修复缺失的字段,因为feature和source字段是在后来添加的 老的数据中不包含这部分字段,所以对老的数据需要人为的添加这两个字段
	 * 
	 * @param doc
	 */
	private void fixLstFtrErr(Document doc) {
		if (doc.containsField(WeiboConsts.ATT_WB_SOURCE) == false) {
			doc.putField(WeiboConsts.ATT_WB_SOURCE, "新浪微博");
		}
		if (doc.containsField(WeiboConsts.ATT_WB_FEATURE) == false) {
			Pattern pattern = Pattern.compile("转发微博|轉發微博|Repost|//");
			Matcher mtch = pattern.matcher(doc.get(WeiboConsts.ATT_WB_TXT));
			if (mtch.find()) {
				doc.putField(WeiboConsts.ATT_WB_FEATURE, "0");
			} else {
				doc.putField(WeiboConsts.ATT_WB_FEATURE, "1");
			}
		}
	}

	public void statisticTm(Info info, int time) {
		info.period.add(Math.abs(info.pretime - time));
		info.pretime = time;
	}

	private boolean statisticRps(Info info, String type) {
		if (type.equals("0")) {
			info.rps++;
			return true;
		}
		return false;
	}

	private boolean statisticUrl(Info info, String content) {

		// TODO 去掉勋章，来自于
		Pattern pattern = Pattern.compile("勋章|我在");
		Matcher m = pattern.matcher(content);
		if (m.find())
			return false;
		pattern = Pattern.compile("http:[^ |^,|^，]*");
		m = pattern.matcher(content);
		if (m.find()) {
			return true;
		}
		return false;
	}

	private void statisticAt(Info info, Document doc) {

		if (doc.containsField(WeiboConsts.ATT_WB_AT)) {
			String atstring = doc.get(WeiboConsts.ATT_WB_AT);
			String[] usrs = atstring.split(StringUtil.STR_DELIMIT_1ST);
			for (String usr : usrs) {
				if (!info.atusr.containsKey(usr))
					info.atusr.put(usr, 0);
				info.atusr.put(usr, info.atusr.get(usr) + 1);
			}
			info.atusertweets++;
		}
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		if (cmdLine.hasOption("usr") && cmdLine.hasOption("wb"))
			return true;
		return false;
	}

	public static void main(String[] args) {
		AdvCli.initRunner(args, cmdName, new LocalRobotAnalyze());
	}

	class Info {
		public String type;
		public int pretime = 0;
		public int evidence = 0;
		public int amount = 0;
		public int friends_count = 0;
		public int rps = 0;
		public int urltweets = 0;
		public int followers_count = 0;
		public int statuses_count = 0;
		public int atusertweets = 0;
		public HashMap<String, Integer> atusr = new HashMap<String, Integer>();
		public String uid = null;
		public ArrayList<Integer> period = new ArrayList<Integer>();

		public Info(String uid) {
			this.uid = uid;
		}
	}

}
