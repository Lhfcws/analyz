package com.yeezhao.analyz.tweet;

public class TweetConsts {
	// input attribute
	public static final String ATT_TW_NICKNAME = "WB_USER"; //input attribute: the sender's nickname of this tweet
	public static final String ATT_TW_TXT = "WB_TXT";	//input attribute: the tweet text
	public static final String ATT_WB_LBL = "WB_LBL"; 	// output attribute: the tweet label
	
	//系统支持的消息类型
	public static enum LabelType { 
		QST("咨询", 1.5), 
		CMP("投诉", 2),
		ACT("微活动", 1.5),
		SRP("纯转发", 1);
		
		private String label; 
		private double weight;
		
		private LabelType(String label, double weight ){
			this.label = label; 
			this.weight = weight; 
		}

		public String getLabel() {
			return label;
		}

		public double getWeight() {
			return weight;
		}
		
		public static LabelType fromLabel(String label){
			if( label.equals(QST.getLabel()))
				return QST;
			else if( label.equals(CMP.getLabel()))
				return CMP;
			else if( label.equals(ACT.getLabel()))
				return ACT;
			else if( label.equals(SRP.getLabel()))
				return SRP;
			else 
				return SRP;
		}
	}

	public static enum WordType{
		OBJ("对象"), 
		ADJ("修饰词"),
		NEG("负面词"),
		POS("正面词"),
		NOISE("干扰词"),
		QST("疑问词"),
		TONE("语气词"),
		COMB("组合词"),
		EMOTN("表情"),
		PRVTV("否定词");
		
		private String type; //type in model file
		
		private WordType(String type){ this.type = type; }
		
		public String getType(){ return type; }
		
		public static WordType fromType(String type){
			if( type.equals(OBJ.getType()))
				return OBJ;
			else if( type.equals(ADJ.getType()))
				return ADJ;
			else if( type.equals(NEG.getType()))
				return NEG;
			else if( type.equals(POS.getType()))
				return POS;
			else if( type.equals(NOISE.getType()))
				return NOISE;
			else if( type.equals(QST.getType()))
				return QST;
			else if( type.equals(TONE.getType()))
				return TONE;
			else if( type.equals(COMB.getType()))
				return COMB;
			else 
				return null;
		}
	}
	
}
