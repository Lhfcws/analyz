package com.yeezhao.analyz.util;

import org.apache.hadoop.hbase.util.Bytes;

public class AppConsts {

	public static final String MYSQL_WEIBO_URL = "mysql.weibo.url";
	
	// Table constants
	public static final String HTBL_USER_PROFILE = "hbase.tbl.profile";
	public static final String HTBL_USER_FANS = "hbase.tbl.fans";
	public static final String HTBL_USER_INTRAC = "hbase.tbl.interaction";

	// Parameter consts
	public static final String FILE_HBASE_CONFIG = "hbase-site.xml";
	public static final String FILE_ANALYZER_CONFIG = "analyzer-config.xml";

	// incrementtalTransfer
	public static final String OBJ_FILE_SUFFIX = ".obj";
	public static final String SQL_FILE_SUFFIX = ".sql";

	// Table column names
	// Input columns
	public static final String COL_USR_NICKNAME = "crawl:nickname";
	public static final String COL_USR_NAME = "crawl:name";
	public static final String COL_USR_GENDER = "crawl:gender";
	public static final String COL_USR_HEAD = "crawl:head";
	public static final String COL_USR_VERIFEDTYPE = "crawl:verified_type";
	public static final String COL_USR_FRIENDCOUNT = "crawl:friends_count";
	public static final String COL_USR_FOLLOWCOUNT = "crawl:followers_count";
	public static final String COL_USR_TWEETNUM = "crawl:tweetsNum";
	public static final String COL_USR_STATUSCOUNT = "crawl:statuses_count";
	public static final String COL_FP_TXT = "fp:content";
	public static final String COL_FP_TIME = "fp:time";
	public static final String COL_FP_FEATURE = "fp:feature";
	public static final String COL_FP_SOURCE = "fp:weiboSource";
	public static final String COL_FP_URL = "fp:url";
	public static final String COL_ANALYZ_TAGDIST = "analyz:tag_dist";

	public static final byte[] COL_FMLY_CRAWL = Bytes.toBytes("crawl");
	public static final byte[] COL_FMLY_ANALYZ = Bytes.toBytes("analyz");
	public static final byte[] COL_FMLY_FRIEND = Bytes.toBytes("friend");
	public static final byte[] COL_FMLY_STATUS = Bytes.toBytes("status");
	public static final byte[] COL_FMLY_FP = Bytes.toBytes("fp");

	public static final byte[] COL_FILTERTYPE = Bytes.toBytes("filter_type");
	public static final byte[] COL_TIME = Bytes.toBytes("time");
	public static final byte[] COL_CONTENT = Bytes.toBytes("content");
	public static final byte[] COL_NICKNAME = Bytes.toBytes("nickname");
	public static final byte[] COL_LOCATION = Bytes.toBytes("location");
	public static final byte[] COL_GENDER = Bytes.toBytes("gender");
	public static final byte[] COL_HEAD = Bytes.toBytes("head");
	public static final byte[] COL_VERIFIEDTYPE = Bytes.toBytes("verified_type");
	public static final byte[] COL_FRIENDCOUNT = Bytes.toBytes("friends_count");
	public static final byte[] COL_FOLLOWCOUNT = Bytes.toBytes("followers_count");
	public static final byte[] COL_STATUSCOUNT = Bytes.toBytes("statuses_count");
	public static final byte[] COL_TWEETNUM = Bytes.toBytes("tweetsNum");
	
	public static final byte[] COL_FETCH = Bytes.toBytes("fetch");
	
	//analyz column
	public static final byte[] COL_AGE = Bytes.toBytes("age");
	public static final byte[] COL_LOCATION_FORMAT = Bytes.toBytes("location_format");
	public static final byte[] COL_USRTYPE = Bytes.toBytes("user_type");
	public static final byte[] COL_USRGROUP = Bytes.toBytes("group_name");
	public static final byte[] COL_META_GROUP = Bytes.toBytes("meta_group");
	public static final byte[] COL_TAG_DIST = Bytes.toBytes("tag_dist");

	// fans qualifier
	public static final byte[] AVGREPOST_QUA = Bytes.toBytes("avg_repost");
	public static final byte[] AVGCOMMENT_QUA = Bytes.toBytes("avg_comment");
	public static final byte[] COMMENTTOTAL_QUA = Bytes.toBytes("comment_total");
	public static final byte[] REPOSTTOTAL_QUA = Bytes.toBytes("repost_total");
	public static final byte[] CRAWLED_FANS = Bytes.toBytes("crawled_fans");

	// Output columns
	public static final String COL_USR_GROUP = "analyz:group_name";
	public static final String COL_USR_TYPE = "analyz:user_type";
	public static final String COL_USR_FILTERTYPE = "analyz:filter_type";
	public static final String COL_USR_AGE = "analyz:age";
	public static final String COL_USR_LOCATIONFORMAT = "analyz:location_format";
	//public static final String COL_USR_EVIDENCE = "evidence";

	// Mysql tables
	public static final String SQL_USER_TX = "t_user_info_tx";
	public static final String SQL_USER_SINA = "t_user_info_sina";
	public static final String SQL_USER_EVID_POST = "_evid";

	// Configuration file constants
	public static final String GROUP_MODEL_FILE = "group.model.file";
	public static final String FILE_UNAME_FILTER = "uname.filter";
	public static final String FILE_AD_FILTER = "adv.filter";

	// Parameter constants
	public static final String CLI_PARAM_T = "t"; // table
	public static final String CLI_PARAM_NOWRITE = "nowrite"; // no write to db

	// Value constants
	public static final String VAL_UNK = "_UNK";
	public static final String LOC_FORMAT_DEFAULT = "其它";
	public static int HBASE_OP_BUF_SIZE = 100;
	public static final int NUM_OF_ACTLVL = 11; // [0-10]

	// User footprint data type
	public static final String FP_MSG = "wb";
	public static final String FP_TAG = "tag";

	public static final String SRC_UID = "src_uid";

	// Operation constants

	public static enum ANALYZ_OP {
		GROUP(5), // groupLabel
		AGE(4), // ageClassify
		UFIL(3), // userFilter
		LOC(2), // locationFormat
		WFIL(1); // weiboFilter
		private int order; // operation执行的顺序,编号由小到大,顺序从前到后

		ANALYZ_OP(int order) {
			this.order = order;
		}

		public int getOrder() {
			return order;
		}
	}

	// hbase parameters
	public static final String MAPRED_JOB_QUEUE_NAME = "mapred.job.queue.name";
	public static final String HBASE_CLIENT_SCANNER_CACHING = "hbase.client.scanner.caching";
	public static final int HBASE_SCANNER_CACHING_SIZE = 50;
	
	public static final String APP_CTEL = "ctel";
	
	public static final int DEFAULT_MAP_NUM = 3;
	
	public static final String ALL_ANALYZ_OP = "UFIL|WFIL|LOC|AGE|GROUP"; 
}
