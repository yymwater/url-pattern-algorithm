package com.flytek.urlpattern.tree.dedupping;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.flytek.urlpattern.tree.CalculateUtil;
import com.flytek.urlpattern.tree.Cluster;
import com.flytek.urlpattern.tree.InputUtil;
import com.flytek.urlpattern.tree.ProcessedUrl;

/**
 * URL重写工具类 
 * @author ymyang2@iflytek.com
 * @date 2014-07-11
 */
public class RewriteTool {
	private static Pattern urlsPattern;
	private static HashMap<String,String>patternMap=new HashMap<String,String>();
	static{
		try {
			BufferedReader reader=new BufferedReader(new FileReader("E://patternAndRules.txt"));
			String lineStr,temp[],patternStr="";
			boolean flag=true;
			while((lineStr=reader.readLine())!=null){
				temp=lineStr.split("\\$\\$");
				if(flag){
					patternStr+=temp[0];
					flag=false;
				}else{
					patternStr+="|"+temp[0];
				}
				patternMap.put(temp[0],temp[1]);
			}
			urlsPattern=Pattern.compile(patternStr);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void main(String argos[]){
		List<Cluster> clusterList;
		int sum=0;
		Set<String>set=new HashSet<String>();
		try {
			clusterList = InputUtil.getClustersFromFile("E:/cluster.txt");
			for(Cluster cluster:clusterList){
				for(ProcessedUrl url:cluster.getUrlList()){
					sum++;
					set.add(rewriteUrl(url.getUrlStr()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("所有URL的数目："+sum);
		System.out.println("重写之后URL的数目："+set.size());
	}
	//改写URL，如果有对应规则，则返回改写后的结果，否则直接返回urlStr，
	public static String rewriteUrl(String urlStr) throws MalformedURLException{
		String processedUrlStr=CalculateUtil.getSortedUrl(urlStr);
		if(!urlsPattern.matcher(processedUrlStr).find()){//
//			System.out.println("没找到匹配的URL！");
			return urlStr;
		}
		ProcessedUrl processedUrl=new ProcessedUrl(urlStr);
		Iterator<String>iterator=patternMap.keySet().iterator();
		String patternStr=null;
		while(iterator.hasNext()){
			patternStr=iterator.next();
			if(Pattern.matches(patternStr,processedUrlStr)){
				break;
			}
		}
		if(patternStr==null){
			return urlStr;
		}
		String patternArr[]=patternMap.get(patternStr).split("/");
		List<String>patternKeyList=new ArrayList<String>();
		HashMap<String,String>map=new HashMap<String, String>();
		for(int i=0;i<patternArr.length;i++){
			String tempArr[]=patternArr[i].split(":");
			patternKeyList.add(tempArr[0]);
			map.put(tempArr[0],tempArr[1]);
		}
		String resultStr="http:/",keyStr,valueStr;
		String patternKeyArr[]=CalculateUtil.sortList(patternKeyList);
		for(int i=0;i<patternArr.length;i++){
			keyStr=patternKeyArr[i];
			if(keyStr==null||keyStr.length()==0)
				continue;
			valueStr=map.get(keyStr);
			int flag;
			if (i > 0) {
				flag = checkFlag(patternKeyArr[i - 1], patternKeyArr[i]);
			} else {
				flag = checkFlag(null, patternKeyArr[i]);
			}
			if(valueStr.startsWith("#")){//keep
				resultStr += getStandardKeyValueStr(patternKeyArr[i], valueStr.substring(1), flag);
			}else if(valueStr.startsWith("*")){//ignore
				resultStr += getStandardKeyValueStr(patternKeyArr[i], valueStr.substring(1), flag);
			}else{//replace
				resultStr += getStandardKeyValueStr(patternKeyArr[i], processedUrl.getValueOfTheKeyInOneUrl(valueStr), flag);
			}
		}
		return resultStr;
	}

	public static String getStandardKeyValueStr(String keyStr, String valueStr,
			int flag) {
		String resultStr = "";
		if (flag == 0) {
			resultStr += "/" + valueStr;
		} else if (flag == 1) {
			resultStr += "?" + keyStr + "=" + valueStr;
		} else {
			resultStr += "&" + keyStr + "=" + valueStr;
		}
		return resultStr;
	}

	// 用于输出pattern(考虑？和&)
	public static int checkFlag(String previousKeyStr, String keyStr) {
		int flag = 0;
		if (keyStr.startsWith("path")) {
			flag = 0;
		} else if (previousKeyStr.startsWith("path")
				&& !keyStr.startsWith("path")) {
			flag = 1;
		} else {
			flag = 2;
		}
		return flag;
	}
}
