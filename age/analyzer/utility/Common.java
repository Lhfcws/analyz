package com.yeezhao.analyz.age.analyzer.utility;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
//import org.bson.BSON;
//import org.bson.BSONObject;
//import org.bson.BasicBSONObject;

public class Common {
	
	public static final Logger summbaLogger = Logger.getLogger(Common.class);
	public static List <String> acList;
	public static int iCount = 0;
	
	public static BufferedReader getFileInfo(String strPath){
		BufferedReader fileBR = null;
		try{
			FileInputStream file = new FileInputStream(strPath);
	        InputStreamReader fileR = new InputStreamReader(file,"utf-8");
	        fileBR = new BufferedReader(fileR);
		}catch(Exception e){
			summbaLogger.info("Error Message : "+e.getMessage()+
					"| Error Stack Trace is :"+e.getStackTrace()); 
			e.printStackTrace();
		}
		return fileBR;
	}
	
	public static List <String> bufferedReaderToList( BufferedReader inStrm ){
		List <String> strList = new ArrayList <String>();
		try{
			BufferedReader tempStrm = inStrm;
			String temp = "";
            while((temp = tempStrm.readLine()) != null){
            	//summbaLogger.info("temp :"+temp);
            	strList.add(temp);
            	//summbaLogger.info("temp : |"+temp); 
            }
		}catch( Exception e){
			summbaLogger.info("Error Message : "+e.getMessage()+
					"| Error Stack Trace is :"+e.getStackTrace()); 
			e.printStackTrace();	
		}
		return strList;
	}
	
	public static void appendFile( String File ,String content ){
		try{
			//String File = "";
			OutputStreamWriter out;
			out = new OutputStreamWriter( new FileOutputStream(File,true),"utf-8");
			out.write( content );
			out.flush();
			out.close();
		}catch( Exception e){
			summbaLogger.info("Error Message : "+e.getMessage()+
					"| Error Stack Trace is :"+e.getStackTrace()); 
			e.printStackTrace();
		}
	}
	
	public static String getBatchID(){
		long begin = System.currentTimeMillis(); 
		String temp = String.valueOf(begin);
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");   
		Date currentTime = new Date();//得到当前系统时间   
		String timeStr_1 = formatter.format(currentTime)+
		temp.substring(temp.length()-3, temp.length()); //将日期时间格式化   
		//long end = System.currentTimeMillis();   
		//System.out.println("[1] " + timeStr_1 );
		return timeStr_1;
	}
	
	public static void addClient( BufferedReader acBR){
		try{
			List <String> strList = new ArrayList <String>();
			strList = Common.bufferedReaderToList(acBR);
			acList = strList;
			//Object[] o = strList.toArray();
			/*
			for(int i=0 ; i <strList.size();i++){
				//strList.g
				acArray[i] = strList.get(0);
			}
			*/
		}catch( Exception e){
			summbaLogger.info("Error Message : "+e.getMessage()+
					"| Error Stack Trace is :"+e.getStackTrace()); 
			e.printStackTrace();
		}
	}
	
	public static String getCurrentClient(){
		String acString = "";
		try{
			//System.out.println("iCount"+iCount);
			//System.out.println("acList.size()"+acList.size());
			if( iCount < acList.size() ){
				summbaLogger.info("get the iCount"+iCount);
				System.out.println("get the iCount"+iCount);
				acString = acList.get(iCount);
				iCount++;
			}else{
				summbaLogger.info("All End");
				System.out.println("All End");
				iCount = 0;
				Thread.sleep(3600*1000);
				acString = acList.get(iCount);
			}
		}catch( Exception e){
			summbaLogger.info("Error Message : "+e.getMessage()+
					"| Error Stack Trace is :"+e.getStackTrace()); 
			e.printStackTrace();
		}
		return acString;
	}
	
	public static String getTagData( String Data,String tag ){
		String strData = "";
		try{
			String temp = Data;
			//String temp2 = null;
			//int inTagLen = tag.length();
			String strKeyword1 = "<"+tag+">";
			String strKeyword2 = "</"+tag+">";
			int beginIndex = temp.indexOf(strKeyword1) + strKeyword1.length();
			//summbaLogger.info("beginIndex : "+beginIndex);
			//summbaLogger.info("temp1 : "+temp1);	
			temp = temp.substring(beginIndex);
			int endIndex = temp.indexOf(strKeyword2);
			///summbaLogger.info("endIndex : "+endIndex);
			temp = temp.substring(0, endIndex);
			strData = temp;
			summbaLogger.info( tag+" : "+strData);
		}catch( Exception e ){
			summbaLogger.info("Error Message : "+e.getMessage()+
					"| Error Stack Trace is :"+e.getStackTrace()); 
			e.printStackTrace();
		}
		return strData;
	}
	
	public static byte[] intToByteArray(int value) {
        return new byte[] {
                (byte)(value >>> 24),
                (byte)(value >>> 16),
                (byte)(value >>> 8),
                (byte)value};
	}
	
	public static byte[] charToByteArray(char value) {
        return new byte[] {
                (byte)(value >>> 8),
                (byte)value};
	}
	
	public static char ByteArrayToChar( byte[] value){
		return (char)(value[1] | value[0] << 8);
		
	}
	
	public static int ByteArrayToInt( byte[] value ){
		//return (int)(value[0] | value[1] << 8 | value[2] << 16 | value[3] << 24);
		//return (int)(value[3] 0xff | value[2] << 8 | value[1] << 16 | value[0] << 24);
		int targets = (value[3] & 0xff) | ((value[2] << 8) & 0xff00) // | 表示安位或
		| ((value[1] << 24) >>> 8) | (value[0] << 24); 
		return targets;
		/*
		int result = 0;
	    for (int i=0; i<4; i++) {
	      result = ( result << 8 ) - Byte.MIN_VALUE + (int) value[i];
	    }
	    return result;
		*/
	}
	public static int ByteArrayToInt2( byte[] value ){
		//return (int)(value[0] | value[1] << 8 | value[2] << 16 | value[3] << 24);
		return (int)(value[3]  | value[2] << 8 | value[1] << 16 | value[0] << 24);
		//int targets = (value[3] & 0xff) | ((value[2] << 8) & 0xff00) // | 表示安位或
		//| ((value[1] << 24) >>> 8) | (value[0] << 24); 
		//return targets;
		/*
		int result = 0;
	    for (int i=0; i<4; i++) {
	      result = ( result << 8 ) - Byte.MIN_VALUE + (int) value[i];
	    }
	    return result;
		*/
	}
	
	public static short ByteArrayToShort( byte[] value ){
		return (short)( value[1] | value[0] << 8);
	}
	
	public static byte[] shortToByteArray( short value ){
		return new byte[] {
                (byte)(value >>> 8),
                (byte)value};
	}
	
	public static short parseCommand( InputStream InStream){
		short shCmd = 0;
		try{
			InputStream tempin = InStream;
			byte[] bCommand = null;
			tempin.read(bCommand);
			//cmd
    		byte bCmdTemp[] = {bCommand[3],bCommand[4]};
    		short cmd = Common.ByteArrayToShort(bCmdTemp);
    		System.out.println("cmd is "+ cmd);
    		return cmd;
		}catch(Exception e){
			summbaLogger.info("Error Message : "+e.getMessage()+
					"| Error Stack Trace is :"+e.getStackTrace()); 
			e.printStackTrace();
		}
		return shCmd;
	}
	
	/*
	public static BSONObject parseBody( InputStream InStream){
		BSONObject obj = new BasicBSONObject();
		try{
			InputStream tempin = InStream;
			byte[] bCommand = null;
			tempin.read(bCommand);
    		//cmd
    		byte bCmdTemp[] = {bCommand[3],bCommand[4]};
    		short cmd = common.ByteArrayToShort(bCmdTemp);
    		System.out.println("cmd is "+ cmd);
    		//len
    		byte bLenTemp[] = {bCommand[5],bCommand[6],bCommand[7],bCommand[8]};
    		int len = common.ByteArrayToInt(bLenTemp);
    		System.out.println("len is "+len);
			//body
    		byte bBody[] = null;
    		for (int i = 0 ; i < len ; i++) {
				bBody[i] = bCommand[i+17];
			}
    		BSONObject bSonObj = BSON.decode(bBody);
    		obj = bSonObj;
    		System.out.println("Body is" + bSonObj);
    		if( cmd == 100){
        		String username = (String) bSonObj.get("username");
        		String strType = (String) bSonObj.get("type");
        		String strSetting = (String) bSonObj.get("setting");
        		System.out.println("username" + username);
        		System.out.println("strType" + strType);
        		System.out.println("strSetting" + strSetting);
        		//return cmd;
    		}
    		return obj;
		}catch(Exception e){
			summbaLogger.info("Error Message : "+e.getMessage()+
					"| Error Stack Trace is :"+e.getStackTrace()); 
			e.printStackTrace();
		}
		return obj;
	}
	*/
}
