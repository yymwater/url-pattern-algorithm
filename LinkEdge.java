package com.flytek.urlpattern.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * author:Yang Yiming
 * 两个相近的node之间的边，表示泛化关系，用于url重写过程
 * date:2014.05.23
 * **/
public class LinkEdge implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 914552716024197267L;
	private TreeNode startNode;//起始节点
	private TreeNode endNode;//终止节点
	private String patternStr;
	private int mapKeyNum;
	private double overlapRate;
	public LinkEdge(TreeNode node1,TreeNode node2){
		startNode=node1;
		endNode=node2;
	}
	public TreeNode getStartNode() {
		return startNode;
	}
	public void setStartNode(TreeNode startNode) {
		this.startNode = startNode;
	}
	public TreeNode getEndNode() {
		return endNode;
	}
	public void setEndNode(TreeNode endNode) {
		this.endNode = endNode;
	}
	public String getPatternStr() {
		return patternStr;
	}
	public void setPatternStr(String patternStr) {
		this.patternStr = patternStr;
	}
	public int getMapKeyNum() {
		return mapKeyNum;
	}
	public void setMapKeyNum(int mapKeyNum) {
		this.mapKeyNum = mapKeyNum;
	}
	public double getOverlapRate() {
		return overlapRate;
	}
	public void setOverlapRate(double overlapRate) {
		this.overlapRate = overlapRate;
	}
	public boolean equals(Object obj) {
		LinkEdge linkEdge=(LinkEdge)obj;
		if(startNode.equals(linkEdge.getStartNode())){
			if(endNode.equals(linkEdge.getEndNode())){
				return true;
			}
		}
		return false;
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
	
	public String rewrite(String url){
		ProcessedUrl processedUrl=new ProcessedUrl(url);
//		System.out.println("patternStr:  "+patternStr);
//		System.out.println(startNode.id+" "+endNode.id+" "+mapKeyNum);
		String patternArr[]=patternStr.split("/");
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
//				resultStr += getStandardKeyValueStr(patternKeyArr[i], valueStr, flag);
				resultStr += getStandardKeyValueStr(patternKeyArr[i], valueStr.substring(1), flag);
			}else if(valueStr.startsWith("*")){//ignore
//				resultStr += getStandardKeyValueStr(patternKeyArr[i], valueStr, flag);
				resultStr += getStandardKeyValueStr(patternKeyArr[i], valueStr.substring(1), flag);
			}else{//replace
				resultStr += getStandardKeyValueStr(patternKeyArr[i], processedUrl.getValueOfTheKeyInOneUrl(valueStr), flag);
			}
		}
//		System.out.println("endNode pattern:"+endNode.getPatternOutputStr());
		return resultStr;
	}
}
