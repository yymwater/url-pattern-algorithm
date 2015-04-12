package com.flytek.urlpattern.tree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.iflytek.net.DomainUtils;
/**
 * author:Yang Yiming
 * 处理后的url类，其中包含了key-value对
 * date:2014.05.23
 * **/
public class ProcessedUrl implements Serializable {
	/**
	 * 
	 */
	private String urlStr;
	private List<KeyValuePair> keyValuePairs;//Key:Value对
	private List<TreeNode> belongToNodeList;
	
	final public String getUrlStr() {
		return urlStr;
	}
	final public void setUrlStr(String urlStr) {
		this.urlStr = urlStr;
	}
	final public List<KeyValuePair> getKeyValuePairs() {
		return keyValuePairs;
	}
	final public void setKeyValuePairs(List<KeyValuePair> keyValuePairs) {
		this.keyValuePairs = keyValuePairs;
	}
	final public void addToBelongToNodeList(TreeNode treeNode){
		belongToNodeList.add(treeNode);
	}
	final public List<TreeNode> getBelongToNodeList() {
		return belongToNodeList;
	}
	final public void setBelongToNodeList(List<TreeNode> belongToNodeList) {
		this.belongToNodeList = belongToNodeList;
	}
	public ProcessedUrl(){
		belongToNodeList=new ArrayList<TreeNode>();
	}
	public ProcessedUrl(String url){
		this.urlStr=url;
		try {
			keyValuePairs=getKeyValuePairsFromUrl();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		belongToNodeList=new ArrayList<TreeNode>();
	}
	
	//重写toString方法
	public String toString() {
		String str=urlStr;
		for(KeyValuePair pair:keyValuePairs){
			str+="\n"+pair;
		}
		return str;
	}
	//重写equals方法
	public boolean equals(Object obj) {
		ProcessedUrl processedUrl=(ProcessedUrl)obj;
		return urlStr.equals(processedUrl.getUrlStr());
	}
	//获取该URL对应的所有keyValuePair http://001ye.wap.blog.163.com/w2/aboutMe.do?hostID=001ye
	public List <KeyValuePair> getKeyValuePairsFromUrl() throws MalformedURLException{
//		String str[]=urlStr.split(":|/|\\?|&");//利用：/?&切分字符串
		List <KeyValuePair> keyValuePairs=new ArrayList<KeyValuePair>();
		int pathNum=0;
		//处理URL的host部分
		URL url=new URL(urlStr);
		String host=url.getHost();
		KeyValuePair pair=new KeyValuePair();
		pair.setKeyStr("path"+pathNum);
		pair.setValueStr(host);
		keyValuePairs.add(pair);
		pathNum++;
		//处理URL的其他部分
		int index=urlStr.indexOf(host);
		String leftUrlStr=urlStr.substring(index+host.length());
		String str[]=StringUtils.split(leftUrlStr, ":/?&.-_");
		String lastKeyStr="";
		HashMap<String,Integer>map=new HashMap<String, Integer>();
		for(String componentStr:str){//componentStr是分割出来的字符串
			if(componentStr.equals(""))
				continue;
			pair=new KeyValuePair();
//			System.out.println(leftUrlStr);
//			if(!componentStr.contains("=")){
			if(!StringUtils.contains(componentStr,'=')){//处理以path开始的key
				index=leftUrlStr.indexOf(componentStr);
				int temp=1;
				if(map.get(lastKeyStr)==null){
					map.put(lastKeyStr,temp);
				}else{
					temp=map.get(lastKeyStr)+1;
					map.put(lastKeyStr,temp);
				}
				if(index>=1&&leftUrlStr.charAt(index-1)=='.'){//判断是否是以.分割开的
					if(lastKeyStr!=null&&!lastKeyStr.startsWith("path"))
						pair.setKeyStr(lastKeyStr+"#1"+temp+"#");
					else
						pair.setKeyStr("path"+pathNum+"#1#");
				}else if(index>=1&&leftUrlStr.charAt(index-1)=='-'){//判断是否是以-分割开的
					if(lastKeyStr!=null&&!lastKeyStr.startsWith("path"))
						pair.setKeyStr(lastKeyStr+"#2"+temp+"#");
					else
						pair.setKeyStr("path"+pathNum+"#2#");
				}else if(index>=1&&leftUrlStr.charAt(index-1)=='_'){//判断是否是以_分割开的
					if(lastKeyStr!=null&&!lastKeyStr.startsWith("path"))
						pair.setKeyStr(lastKeyStr+"#3"+temp+"#");
					else
						pair.setKeyStr("path"+pathNum+"#3#");
				}
				else{
					pair.setKeyStr("path"+pathNum);
					lastKeyStr=pair.getKeyStr();
				}
				Pattern pattern=Pattern.compile("^([^\\d]+)(\\d+)$");
				Matcher matcher=pattern.matcher(componentStr);
				if(!matcher.find()){//不是字母字符串+数字的格式
					pair.setValueStr(componentStr);					
				}else{//字母字符串+数字的格式
					String letterStr=matcher.group(1);
					String numStr=matcher.group(2);
					pair.setValueStr("#"+letterStr+"$"+numStr+"#");
				}
				pathNum++;
			}else{//处理不以path开始的key
				try{
//					String s[]=componentStr.split("=");
					String s[]=StringUtils.split(componentStr,'=');
					if(s.length==2&&s[0].length()!=0&&s[1].length()!=0){
						pair.setKeyStr(s[0]);
						pair.setValueStr(s[1]);
					}else{
						Pattern pattern=Pattern.compile("^([^\\d]+)(\\d+)$");
						Matcher matcher=pattern.matcher(componentStr);
						//判断是否是以.分割开的
						index=leftUrlStr.indexOf(componentStr);
						if(index>=1&&leftUrlStr.charAt(index-1)=='.'){
							pair.setKeyStr("path"+pathNum+"#");
						}
						else{ 
							pair.setKeyStr("path"+pathNum);
						}
						if(!matcher.find()){//不是字母字符串+数字的格式
							pair.setValueStr(componentStr);					
						}else{//字母字符串+数字的格式
							String letterStr=matcher.group(1);
							String numStr=matcher.group(2);
							pair.setValueStr("#"+letterStr+"$"+numStr+"#");
						}
						pathNum++;
					}
				}catch(Exception e){
					System.out.println(urlStr);
					e.printStackTrace();
					continue;
				}
				lastKeyStr=pair.getKeyStr();
			}
//			leftUrlStr=leftUrlStr.replace(componentStr,"");
			leftUrlStr=leftUrlStr.substring(componentStr.length()+1);
//			System.out.println(leftUrlStr);
//			System.out.println();
			
			keyValuePairs.add(pair);
		}
		return keyValuePairs;
	}
	public static void main(String argos[]){	
		ProcessedUrl url=new ProcessedUrl("http://wap.gmw.cn/bj/diannao/?ifid=ganji_shouye_remen_diannao");
		System.out.println(url.getUrlStr());
		for(KeyValuePair pair:url.getKeyValuePairs()){
			System.out.println(pair.getKeyStr()+":"+pair.getValueStr());
		}
	}
	
	//返回某一ProcessedUrl中key对应的value值，若无此key则返回null
	public String getValueOfTheKeyInOneUrl(String key){
		for(KeyValuePair pair:keyValuePairs){
			if(pair.getKeyStr().equals(key)){
				return pair.getValueStr();
			}
		}
		return null;
	}
	
	//返回一个ProcessUrl列表中URL对应的key的value值的列表(已去除重复value值) 考虑字母字符串+数字的格式只取字母字符串部分
	public static List<String> getValuesInTheKey(String key,List<ProcessedUrl>urlsList){
		Set<String>valuesSet=new HashSet<String>();
		for(ProcessedUrl url:urlsList){
			String valueStr=url.getValueOfTheKeyInOneUrl(key);
			if(valueStr!=null){
				if(valueStr.startsWith("#")&&valueStr.endsWith("#")){
					int index=valueStr.indexOf("$");
					valuesSet.add(valueStr.substring(0, index)+"#");
				}else{
					valuesSet.add(valueStr);					
				}
			}
		}
		List<String>valuesList=new ArrayList<String>();
		Iterator<String>iterator=valuesSet.iterator();
		while(iterator.hasNext()){
			valuesList.add(iterator.next());
		}
		return valuesList;
	}
	//返回一个ProcessUrl列表中URL对应的key的value值的列表(已去除重复value值)
	public static List<String> getValuesInTheKey1(String key,List<ProcessedUrl>urlsList){
		Set<String>valuesSet=new HashSet<String>();
		for(ProcessedUrl url:urlsList){
			String valueStr=url.getValueOfTheKeyInOneUrl(key);
			if(valueStr!=null){
				valuesSet.add(valueStr);					
			}
		}
		List<String>valuesList=new ArrayList<String>();
		Iterator<String>iterator=valuesSet.iterator();
		while(iterator.hasNext()){
			valuesList.add(iterator.next());
		}
		return valuesList;
	}
	
	//返回一个ProcessUrl列表中URL对应的key出现次数超过95%的value
	public static String getMostedValuesInTheKey2(String key,List<ProcessedUrl>urlsList){
		HashMap<String,Integer>map=new HashMap<String, Integer>();
		for(ProcessedUrl url:urlsList){
			String valueStr=url.getValueOfTheKeyInOneUrl(key);
			if(valueStr!=null){
				String insertValueStr=valueStr;
				if(valueStr.startsWith("#")&&valueStr.endsWith("#")){
					int index=valueStr.indexOf("$");
					insertValueStr="##"+valueStr.substring(0, index)+"#";
				}
				if(map.get(insertValueStr)!=null){//计数
					map.put(insertValueStr, map.get(insertValueStr)+1);
				}else{
					map.put(insertValueStr,1);
				}
			}
		}
		List<String>valuesList=new ArrayList<String>();
		Iterator<String>iterator=map.keySet().iterator();
		String valueStr;int sum=0;
		while(iterator.hasNext()){//遍历求和
			valueStr=iterator.next();
			sum+=map.get(valueStr);
		}
		iterator=map.keySet().iterator();
		int temp;
		while(iterator.hasNext()){//遍历
			valueStr=iterator.next();
			temp=map.get(valueStr);
			if(temp*1.0/sum>0.7){//&&sum>100
				if(valueStr.startsWith("##")){
					return valueStr.substring(2)+"\\d";
				}else{
					return valueStr;
				}
			}
		}
		return null;
	}
	
	//判断该ProcessedUrl中某key对应的值是否为value(注：如果URL中没有key则也返回false)
	public static boolean isUrlHasTheValueInTheKey(ProcessedUrl url,String key,String value){
		for(KeyValuePair pair:url.getKeyValuePairs()){
			if(pair.getKeyStr().equals(key)){
				if(pair.getValueStr().equals(value))
					return true;
				return false;
			}	
		}
		return false;
	}
	
	//返回一个ProcessedUrl列表中，针对一个key值,所有value值及其出现次数的map
	public static HashMap<String,Integer>getValuesAndTimesMapForOneKey(List<ProcessedUrl> urlList,String key){
		HashMap<String, Integer>valueAndTimesMap=new HashMap<String,Integer>();
		for(ProcessedUrl processedUrl:urlList){//遍历urlList
			String valueStr=processedUrl.getValueOfTheKeyInOneUrl(key);
			if(valueStr==null){//该url中不包含此key
				continue;
			}
			if(valueStr.startsWith("#")&&valueStr.endsWith("#")){
				int index=valueStr.indexOf("$");
				valueStr=valueStr.substring(1, index);
			}
			if(Pattern.matches("\\d+",valueStr))
				valueStr="\\d*";
			if(valueAndTimesMap.get(valueStr)==null){
				valueAndTimesMap.put(valueStr, 1);
			}else {
				int timesTemp=valueAndTimesMap.get(valueStr)+1;
				valueAndTimesMap.put(valueStr,timesTemp);
			}
		}
		return valueAndTimesMap;
	}
	
	//返回一个ProcessedUrl的列表中的所有URL字符串
	public static List<String>getUrls(List<ProcessedUrl>processedUrlList){
		Set<String>urlSet=new HashSet<String>();
		for(ProcessedUrl processedUrl:processedUrlList){
			urlSet.add(processedUrl.getUrlStr());
		}
		List<String>urlList=new ArrayList<String>();
		Iterator<String>iterator=urlSet.iterator();
		while(iterator.hasNext())
			urlList.add(iterator.next());
		return urlList;
	}
	
	//返回一个URL list中所有出现的所有key
	public static List<String> getKeysFromUrls(List<ProcessedUrl>processedUrlList){
		Set <String> keySet=new HashSet<String>();
		for(ProcessedUrl url:processedUrlList){
			for(KeyValuePair pair:url.getKeyValuePairs()){
				keySet.add(pair.getKeyStr());
			}
		}
		Iterator<String>keySetIterator=keySet.iterator();
		List<String>keyList=new ArrayList<String>();
		while (keySetIterator.hasNext()) {
			keyList.add(keySetIterator.next());
		};
		return keyList;
	}
	//返回一个URL list中每个URL均出现的所有key(即共有的key)
	public static List<String> getCommonKeysFromUrls(List<ProcessedUrl>processedUrlList,List<String>keyDoneList){
		Set <String> keySet=new HashSet<String>();
		for(ProcessedUrl url:processedUrlList){
			for(KeyValuePair pair:url.getKeyValuePairs()){
				keySet.add(pair.getKeyStr());
			}
		}
		Iterator<String>keySetIterator=keySet.iterator();
		List<String>keyList=new ArrayList<String>();
		while(keySetIterator.hasNext()) {
			String keyStr=keySetIterator.next();
			Boolean flag=true;
			for(ProcessedUrl url:processedUrlList){
				String value=url.getValueOfTheKeyInOneUrl(keyStr);
				if(value==null){//该key值未出现在url中
					flag=false;
					break;
				}
			}
			if(flag)
				keyList.add(keyStr);
		};
		keySet=null;
		if(keyDoneList!=null){
			for(String keyStr:keyDoneList){
				keyList.remove(keyStr);
			}
		}
		return keyList;
	}
}
