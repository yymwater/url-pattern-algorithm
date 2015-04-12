package com.flytek.urlpattern.tree;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.iflytek.net.DomainUtils;
/**
 * author:Yang Yiming
 * 处理后的url类，其中包含了key-value对，在CalculateUtil.getSortedUrl中使用，用于匹配
 * date:2014.08.18
 * **/
public class ProcessedUrl1 implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8670525953754323568L;
	private String urlStr;
	private List<KeyValuePair> keyValuePairs;//Key:Value对
	private List<TreeNode> belongToNodeList;
	
	public String getUrlStr() {
		return urlStr;
	}
	public void setUrlStr(String urlStr) {
		this.urlStr = urlStr;
	}
	public List<KeyValuePair> getKeyValuePairs() {
		return keyValuePairs;
	}
	public void setKeyValuePairs(List<KeyValuePair> keyValuePairs) {
		this.keyValuePairs = keyValuePairs;
	}
	public void addToBelongToNodeList(TreeNode treeNode){
		belongToNodeList.add(treeNode);
	}
	public List<TreeNode> getBelongToNodeList() {
		return belongToNodeList;
	}
	public void setBelongToNodeList(List<TreeNode> belongToNodeList) {
		this.belongToNodeList = belongToNodeList;
	}
	public ProcessedUrl1(){
		belongToNodeList=new ArrayList<TreeNode>();
	}
	public ProcessedUrl1(String url){
		this.urlStr=url;
		keyValuePairs=getKeyValuePairsFromUrl();
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
	public List <KeyValuePair> getKeyValuePairsFromUrl(){
		List <KeyValuePair> keyValuePairs=new ArrayList<KeyValuePair>();
//		String str[]=urlStr.split(":|/|\\?|&");//利用：/?&切分字符串
		String str[]=StringUtils.split(urlStr, ":/?&");
		int pathNum=0;
		boolean isFirst=true;
		for(String componentStr:str){
			if(componentStr.equals(""))
				continue;
			if(isFirst){//不考虑协议名
				isFirst=false;
				continue;
			}
			KeyValuePair pair=new KeyValuePair();
//			if(!componentStr.contains("=")){
			if(!StringUtils.contains(componentStr,'=')){
				pair.setKeyStr("path"+pathNum);
				pair.setValueStr(componentStr);
				pathNum++;
			}else{
				try{
//					String s[]=componentStr.split("=");
					String s[]=StringUtils.split(componentStr,'=');
					if(s.length==2&&s[0].length()!=0&&s[1].length()!=0){
						pair.setKeyStr(s[0]);
						pair.setValueStr(s[1]);
					}else{
						pair.setKeyStr("path"+pathNum);
						pair.setValueStr(componentStr);
						pathNum++;
					}
				}catch(Exception e){
					System.out.println(urlStr);
					e.printStackTrace();
					continue;
				}
			}
			keyValuePairs.add(pair);
		}
		return keyValuePairs;
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
	
	//返回一个ProcessUrl列表中URL对应的key的value值的列表(已去除重复value值)
	public static List<String> getValuesInTheKey(String key,List<ProcessedUrl>urlsList){
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
		if(keyDoneList!=null){
			for(String keyStr:keyDoneList){
				keyList.remove(keyStr);
			}
		}
		return keyList;
	}
}
