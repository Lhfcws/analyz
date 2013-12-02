package com.yeezhao.analyz.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.yeezhao.commons.util.sql.BaseDao;
import com.yeezhao.commons.util.sql.BaseDaoFactory;

public class IncrementalToSql {
	static Log LOG = LogFactory.getLog(IncrementalToSql.class);

	private BaseDao baseDao;
	private int batchNum = 1000;
	private Configuration conf;

	public IncrementalToSql(Configuration conf, int batchNum) throws Exception {
		baseDao = BaseDaoFactory.getDaoBaseInstance(conf
				.get(AppConsts.MYSQL_WEIBO_URL));
		this.batchNum = batchNum;
		this.conf = conf;
	}

	public void close() {
	}

	private static final char[] hexDigit = { '0', '1', '2', '3', '4', '5', '6',
			'7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	private static char toHex(int nibble) {

		return hexDigit[(nibble & 0xF)];

	}

	/**
	 * 
	 * 将字符串编码成 Unicode 形式的字符串. 如 "黄" to "\u9EC4"
	 * Converts unicodes to encoded \\uxxxx and escapes
	 * special characters with a preceding slash
	 * @param theString
	 *            待转换成Unicode编码的字符串。
	 * @param escapeSpace
	 *            是否忽略空格，为true时在空格后面是否加个反斜杠。
	 * @return 返回转换后Unicode编码的字符串。
	 */

	public static String toEncodedUnicode(String theString, boolean escapeSpace) {
		int len = theString.length();
		int bufLen = len * 2;
		if (bufLen < 0) {
			bufLen = Integer.MAX_VALUE;
		}

		StringBuffer outBuffer = new StringBuffer(bufLen);
		for (int x = 0; x < len; x++) {
			char aChar = theString.charAt(x);
			// Handle common case first, selecting largest block that
			// avoids the specials below
			if ((aChar > 61) && (aChar < 127)) {
				if (aChar == '\\') {
					outBuffer.append('\\');
					outBuffer.append('\\');
					continue;
				}
				outBuffer.append(aChar);
				continue;
			}

			switch (aChar) {
			case ' ':
				if (x == 0 || escapeSpace)
					outBuffer.append('\\');
				outBuffer.append(' ');
				break;
			case '\t':
				outBuffer.append('\\');
				outBuffer.append('t');
				break;
			case '\n':
				outBuffer.append('\\');
				outBuffer.append('n');
				break;
			case '\r':
				outBuffer.append('\\');
				outBuffer.append('r');
				break;
			case '\f':
				outBuffer.append('\\');
				outBuffer.append('f');
				break;
			case '=': // Fall through
			case ':': // Fall through
			case '#': // Fall through
			case '!':
				outBuffer.append('\\');
				outBuffer.append(aChar);
			break;
			default:
				if ((aChar < 0x0020) || (aChar > 0x007e)) {
					// 每个unicode有16位，每四位对应的16进制从高位保存到低位
					outBuffer.append('\\');
					outBuffer.append('u');
					outBuffer.append(toHex((aChar >> 12) & 0xF));
					outBuffer.append(toHex((aChar >> 8) & 0xF));
					outBuffer.append(toHex((aChar >> 4) & 0xF));
					outBuffer.append(toHex(aChar & 0xF));
				} else {
					outBuffer.append(aChar);
				}
			}
		}
		return outBuffer.toString();

	}

	public int updateOneFile(Path path) {
		int num = 1;
		try {
			List<String> sqlList = new LinkedList<String>();
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			String line = null;
			long totalInsertTime = 0;

			while ((line = br.readLine()) != null) {
				try {
					line = line.trim();
					sqlList.add(line);
					if (num % batchNum == 0) { // 每1000行进行一次commit
						long startTime = System.currentTimeMillis();
						baseDao.batchExecute(sqlList);
						sqlList.clear();
						long endTime = System.currentTimeMillis();
						long oneTime = endTime - startTime;
						totalInsertTime += oneTime;
						LOG.info(path.getName() + ", num: " + num
								+ ", insert time: " + oneTime);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				num++;
			}
			br.close();
			if (!sqlList.isEmpty())
				baseDao.batchExecute(sqlList);
			LOG.info("totalInsertTime: " + totalInsertTime);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("fileName: " + path.getName() + ", update error!");
		}
		LOG.info("fileName: " + path.getName() + ", total: " + num);
		return num;
	}

	public void updateSql(String path) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path root = new Path(path);
		int startTime = (int) (System.currentTimeMillis() / 1000);
		FileStatus[] status = fs.listStatus(root);
		int total = 0;
		for (int i = 0; i < status.length; i++) {
			Path sub = status[i].getPath();
			if (sub.getName().endsWith(AppConsts.SQL_FILE_SUFFIX)) {
				total += updateOneFile(sub);
			}
		}
		int endTime = (int) (System.currentTimeMillis() / 1000);
		LOG.info("total time: " + (endTime - startTime) + ", totalRow: "
				+ total);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// 创建 Options 对象
		Options options = new Options();
		// 添加 -h 参数
		options.addOption("f", true, "filePath");
		options.addOption("n", true, "batchNum");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		// 获取 -f 参数值
		String file = cmd.getOptionValue("f");
		String batchNumStr = cmd.getOptionValue("n");
		int batchNum = 1000;
		if (batchNumStr != null) {
			try {
				batchNum = Integer.parseInt(batchNumStr);
			} catch (Exception e) {
				e.printStackTrace();
				batchNum = 1000;
			}
		}
		if (file == null || file.isEmpty()) {
			System.out
					.println("sh run.sh incrementalToSql -f <filePath> -n <batchNum>");
			return;
		}
		LOG.info("incrementalToSql to file: " + file + ", batchNum: "
				+ batchNum);
		Configuration conf = AnalyzConfiguration.getInstance();
		IncrementalToSql incrementalToSql = new IncrementalToSql(conf, batchNum);
		incrementalToSql.updateSql(file);
		incrementalToSql.close();
	}

}
