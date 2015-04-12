package com.flytek.urlpattern.tree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.xalan.xsltc.compiler.sym;
import org.eclipse.jetty.util.log.Log;

import com.iflytek.common.IIntervalTask;
import com.sun.corba.se.spi.orbutil.fsm.Input;
import com.sun.org.apache.xerces.internal.dom.ChildNode;


/**
 * author:Yang Yiming
 * 主类，用于构建模式树和泛化等操作
 * date:2014.05.23
 * **/
public class Fun {
	private static TreeNode rootNodeOfPatternTree;//记录模式树的根节点
	private static double MINTREESIZE=30;
	public static int nodeId=0;
	public TreeNode getRootNodeOfPatternTree() {
		return rootNodeOfPatternTree;
	}
	public void setRootNodeOfPatternTree(TreeNode rootNodeOfPatternTree) {
		rootNodeOfPatternTree = rootNodeOfPatternTree;
	}
//	static{
//		//读取配置文件中的相关参数
//		String minTreeSizeStr=InputUtil.readProperties("minTreeSize");
//		if(minTreeSizeStr!=null){
//			MINTREESIZE=Double.parseDouble(minTreeSizeStr);
//		}
//		else{//未设置则设为缺省值0
//			MINTREESIZE=0;
//		}
//	}
	public static int sum=0;
	//处理数据中/?的情况
	final public static boolean checkSpecialData(List<ProcessedUrl>urlList){
		boolean flag1=false;
		boolean flag2=false;
		for(ProcessedUrl processedUrl:urlList){
			if(processedUrl.getUrlStr().contains("/?")){
				flag1=true;
			}else{
				flag2=true;
			}
			if(flag1&&flag2){
				return true;
			}
		}
		return false;
	}
	final public static boolean hasSpecialData(List<ProcessedUrl>urlList){
		boolean flag1=false;
		for(ProcessedUrl processedUrl:urlList){
			if(processedUrl.getUrlStr().contains("/?")){
				return true;
			}
		}
		return false;
	}
	/**
	 * 建立模式树的主函数，递归建立
	 * */
	final public TreeNode buildPatternTree(List<ProcessedUrl> urlList,List<String>keyDoneList,TreeNode parentNode,String partitionKeyValueStr){
		sum+=urlList.size();
		//预先处理，把该节点中URL某key对应的所有值都相同，但如果该key不在keyDoneList中，则添加进入，因为此key没必要作为划分属性
		List<String>keyList=ProcessedUrl.getCommonKeysFromUrls(urlList,null);
		List<String>addToKeyDoneList=new ArrayList<String>();
		for(String keyStr:keyList){
			List <String>valueList=ProcessedUrl.getValuesInTheKey(keyStr, urlList);
			if(valueList.size()==1&&keyDoneList.indexOf(keyStr)==-1){
//				System.out.println("valueList size 为1："+keyStr);
				addToKeyDoneList.add(keyStr);
			}
		}
		for(String keyStr:addToKeyDoneList){
			keyDoneList.add(keyStr);
		}
		//预先处理结束
		TreeNode treeNode=new TreeNode(urlList,parentNode,partitionKeyValueStr);
		//处理特殊类型数据：包含/?的数据
		if(checkSpecialData(urlList)){
			List<ProcessedUrl>list1=new ArrayList<ProcessedUrl>();
			List<ProcessedUrl>list2=new ArrayList<ProcessedUrl>();
			for(ProcessedUrl url:urlList){
				if(url.getUrlStr().contains("/?")){
					list1.add(url);
				}else{
					list2.add(url);
				}
			}
			List<String>keysList1=new ArrayList<String>();
			List<String>keysList2=new ArrayList<String>();
			for(String str:keyDoneList){
				keysList1.add(str);
				keysList2.add(str);
			}
			if(list1!=null&&list1.size()>30){
				TreeNode childNode=buildPatternTree(list1,keysList1,treeNode,null);
				treeNode.addChild(childNode);
			}
			if(list2!=null&&list2.size()>30){
				TreeNode childNode=buildPatternTree(list2,keysList2,treeNode,null);
				treeNode.addChild(childNode);
			}
			return treeNode;
		}
		//计算每个Key对应的熵
//		System.out.println("keyDoneList size is "+keyDoneList.size());
//		System.out.println("keyDoneList:");
//		for(String temp:keyDoneList){
//			System.out.println("    "+temp);
//		}
		HashMap<String,Double>keyAndEntropyMap=CalculateUtil.calculateEntropyForEachKey(urlList,keyDoneList);
		String selectedKey=null;
		//选择对应熵最小的Key
		Iterator<String>keyIterator=keyAndEntropyMap.keySet().iterator();
//		System.out.println("key数目："+keyAndEntropyMap.keySet().size());
		double minEntropy=99999999;
		while(keyIterator.hasNext()){
			String key=keyIterator.next();
			double entropyTemp=keyAndEntropyMap.get(key);
//			System.out.println("熵："+key+" "+entropyTemp);
			if(entropyTemp<minEntropy){
				selectedKey=key;
				minEntropy=entropyTemp;
			}
		}
		/**
		if(keyAndEntropyMap.keySet().size()==0){
//			System.out.println("get mosted key");
			selectedKey=CalculateUtil.getMostedKey2(urlList, keyDoneList);//最新
//			selectedKey=CalculateUtil.calculateAndSelectKeyForNoCommonKeys(urlList,keyDoneList);
		}*/
//		System.out.println("划分属性："+selectedKey);
//		System.out.println("-----------");
//		System.out.println("url数目："+urlList.size()+" 划分属性："+selectedKey);
		if(selectedKey!=null){//在该节点利用selectedKey进行划分子节点
			keyDoneList.add(selectedKey);
//			System.out.println("-----");
			treeNode.setPartitionKeyStr(selectedKey);//设置该节点对应的partitionKeyStr
			//该key值下所有value及其对应的出现次数
			HashMap<String,Integer>valueAndTimesMap=ProcessedUrl.getValuesAndTimesMapForOneKey(urlList,selectedKey);
//			for(String value:valueAndTimesMap.keySet()){
//				System.out.println(value+" "+valueAndTimesMap.get(value));
//			}
			//该key值下所有value及其是否是trivial
			HashMap<String,Boolean>valueIsTrivialOrNotMap=CalculateUtil.judgeValuesIsTrivialOrNot(valueAndTimesMap);
//			for(String value:valueIsTrivialOrNotMap.keySet()){
//				System.out.println(value+" "+valueAndTimesMap.get(value)+" "+valueIsTrivialOrNotMap.get(value));
//			}
			valueAndTimesMap=null;//##
			//这里是递归的结束条件:如果selectedKey对应的所有的value均为trivial，则不再向下递归
			Iterator<String>valueIterator =valueIsTrivialOrNotMap.keySet().iterator();
			boolean allIsTrivial=true;//预设为TRUE，即表示所有value值均为trivial
			while(valueIterator.hasNext()){
				String valueStr=valueIterator.next();
				if(!valueIsTrivialOrNotMap.get(valueStr)){
					allIsTrivial=false;
				}
			}
			if(allIsTrivial){
//				System.out.println("All Values Is Trivial!!!");
				return treeNode;
			}
			//该key值下所有value对应的位置，用于划分子集用
			HashMap<String,Integer>valueAndPositionMap=new HashMap<String,Integer>();
			valueIterator=valueIsTrivialOrNotMap.keySet().iterator();
			int index=2;
			while(valueIterator.hasNext()){
				String valueStr=valueIterator.next();
//				System.out.println("@"+valueStr);
				if(valueIsTrivialOrNotMap.get(valueStr)){//trivial values
					if(valueAndPositionMap.get("*")==null){
						valueAndPositionMap.put("*",0);//trivial values的节点在第一个子节点中
					}
				}else{//salient values
					valueAndPositionMap.put(valueStr,index);
//					System.err.println("主要属性："+valueStr);
					index++;
				}
			}
			int urlListListLength=index;
			//将urlList在key属性下根据value的值进行划分子集
			List<List<ProcessedUrl>>urlListList=new ArrayList<List<ProcessedUrl>>();
			for(int i=0;i<urlListListLength;i++)
				urlListList.add(new ArrayList<ProcessedUrl>());
			String valueCorrespondToList[]=new String[urlListListLength];//记录urlListList中第i个元素对应的是哪个value(salient value or *)
			for(ProcessedUrl processedUrl:urlList){
				String valueStr=processedUrl.getValueOfTheKeyInOneUrl(selectedKey);
				if(valueStr==null){
					continue;
				}
				if(valueStr.startsWith("#")&&valueStr.endsWith("#")){
					int indexNum=valueStr.indexOf("$");
					valueStr=valueStr.substring(1, indexNum);
				}
				if(Pattern.matches("\\d+",valueStr))
					valueStr="\\d*";
//				System.out.println(processedUrl);
//				System.out.println("#"+selectedKey);
				//selectedKey为公有的，value不会为空
				if(valueIsTrivialOrNotMap.get(valueStr)){//该节点是trivial
					index=valueAndPositionMap.get("*");
					valueCorrespondToList[index]="*";
					treeNode.setChildHasStarFlag(true);
				}else {
					index=valueAndPositionMap.get(valueStr);
					valueCorrespondToList[index]=valueStr;
				}
				urlListList.get(index).add(processedUrl);
			}
//			System.out.println("The Selected Key is "+selectedKey);
//			System.out.println("The Selected Key has "+valueIsTrivialOrNotMap.keySet().size()+" values!");
//			System.out.println("The node has "+urlListList.size()+" children!");
//			for(int i=0;i<urlListListLength;i++){
//				if(urlListList.get(i).size()>100)
//					System.out.println("   child"+i+" has "+urlListList.get(i).size()+" urls!");
//			}
//			System.out.println("urlListList:"+urlListListLength);
			for(int i=0;i<urlListListLength;i++){
				//递归引用时防止引用出问题
				List<String>keyDoneTempList=new ArrayList<String>();
				for(String str:keyDoneList){
					keyDoneTempList.add(str);
				}//
//				System.out.println(valueCorrespondToList[i]);				
				if(parentNode==null){
					int rootSize=urlList.size();
					if(rootSize>10000000)
						MINTREESIZE=5000;
					else if(rootSize>5000000)
						MINTREESIZE=2000;
					else if(rootSize>1000000)
						MINTREESIZE=500;
					else if(rootSize>500000)
						MINTREESIZE=300;
					else if(rootSize>100000)
						MINTREESIZE=100;
					else if(rootSize>10000)
						MINTREESIZE=50;
				}
//				if(rootNodeOfPatternTree!=null)
//					System.out.println(rootNodeOfPatternTree.getUrlsList().size()+" "+minTreeSize);
				if(urlListList.get(i).size()>=MINTREESIZE){
					TreeNode childNode=buildPatternTree(urlListList.get(i),keyDoneTempList,treeNode,valueCorrespondToList[i]);
					treeNode.addChild(childNode);
				}
			}
		}else{
//			System.out.println("No Selected Key");
			String mostedKeyForNoSplitKey=CalculateUtil.getMostedKeyForNoSplitKey(urlList, keyDoneList);
			if(mostedKeyForNoSplitKey!=null){
				List<ProcessedUrl>list1=new ArrayList<ProcessedUrl>();
				List<ProcessedUrl>list2=new ArrayList<ProcessedUrl>();
				for(ProcessedUrl url:urlList){
					if(url.getValueOfTheKeyInOneUrl(mostedKeyForNoSplitKey)==null){
						list1.add(url);
					}else{
						list2.add(url);					
					}
				}
				List<String>keysList1=new ArrayList<String>();
				List<String>keysList2=new ArrayList<String>();
				for(String str:keyDoneList){
					keysList1.add(str);
					keysList2.add(str);
				}
				if(list1!=null&&list1.size()>30){
					TreeNode childNode=buildPatternTree(list1,keysList1,treeNode,null);
					treeNode.addChild(childNode);
				}
				if(list2!=null&&list2.size()>30){
					TreeNode childNode=buildPatternTree(list2,keysList2,treeNode,null);
					treeNode.addChild(childNode);
				}
			}
		}
		return treeNode;
	}
	public static int getLeafNum(){
		int sum=0;
		Queue<TreeNode> nodeQueue = new LinkedList<TreeNode>();
		TreeNode tempNode;
		nodeQueue.add(rootNodeOfPatternTree);
		while (!nodeQueue.isEmpty()) {
			tempNode = nodeQueue.poll();
			if(tempNode.getChildNodes()==null||tempNode.getChildNodes().size()==0){
				sum+=tempNode.getUrlsList().size();
				continue;
			}
			for (TreeNode childNode : tempNode.getChildNodes()) {
				if(childNode!=null){
					nodeQueue.add(childNode);
				}
			}
		}
		return sum;
	}
	//将ProcessedUrl所属的Node添加到对象中
	public static void prepareForRuleGeneration(){
		Queue<TreeNode> nodeQueue = new LinkedList<TreeNode>();
		TreeNode tempNode;
		nodeQueue.add(rootNodeOfPatternTree);
		while (!nodeQueue.isEmpty()) {
			tempNode = nodeQueue.poll();
			for(ProcessedUrl url:tempNode.getUrlsList()){
				url.addToBelongToNodeList(tempNode);
			}
			if(tempNode.getChildNodes()==null||tempNode.getChildNodes().size()==0){
				continue;
			}
			for (TreeNode childNode : tempNode.getChildNodes()) {
				if(childNode!=null){
					nodeQueue.add(childNode);
				}
			}
		}
	}
	
	//给定所有cluster集合，产生候选规则
	public static void candidateRuleGeneration(List<Cluster>clusterList){
		List<TreeNodePair>treeNodepairsList=TreeNode.identifyDuplicateNodes(rootNodeOfPatternTree,clusterList);
		System.out.println("相似节点对数目为:"+treeNodepairsList.size());
		Iterator<TreeNodePair>treeNodepairsListIterator=treeNodepairsList.iterator();
		while(treeNodepairsListIterator.hasNext()){
			TreeNodePair pair=treeNodepairsListIterator.next();
			TreeNode.rewrite(pair);
		}
	}
	//选择规则
	public static void selectDeloyableRule(){
		//该节点有多条出边
		Queue<TreeNode>nodeQueue=new LinkedList<TreeNode>();
		nodeQueue.add(rootNodeOfPatternTree);
		TreeNode currentNode;
		//情况1：当前节点有多个出边
		while(!nodeQueue.isEmpty()){
			//当前节点
			currentNode=nodeQueue.poll();
			if(currentNode.getFromLinkEdgeList()!=null&&currentNode.getFromLinkEdgeList().size()>1){
				System.out.println("出现多条边"+currentNode.getFromLinkEdgeList().size());
				double overlapRateMax=currentNode.getFromLinkEdgeList().get(0).getOverlapRate();
				System.out.println(overlapRateMax);
				int index=0;//overlap值最大对应的边的位置
				int length=currentNode.getFromLinkEdgeList().size();
				//只保存index对应的边
				for(int i=1;i<length;i++){
					double temp=currentNode.getFromLinkEdgeList().get(i).getOverlapRate();
					System.out.println(temp);
					if(temp==1.0){
						System.out.println("两个节点重复度为1："+currentNode.getFromLinkEdgeList().get(i).getStartNode().id+" "+currentNode.getFromLinkEdgeList().get(i).getEndNode().id);
						System.out.println("   起始节点");
//						for(ProcessedUrl url:currentNode.getFromLinkEdgeList().get(i).getStartNode().getUrlsList()){
//							System.out.println(url.getUrlStr());
//						}
//						System.out.println("   终止节点");
//						for(ProcessedUrl url:currentNode.getFromLinkEdgeList().get(i).getEndNode().getUrlsList()){
//							System.out.println(url.getUrlStr());
//						}
					}
					if(temp>overlapRateMax){
						overlapRateMax=temp;
						index=i;
					}
				}
				/**未考虑JVM垃圾回收*/
				for(int i=0;i<length;i++){//将其他边从EndNode的toLinkEdgeList中删除
					if(i!=index){
						LinkEdge tempEdge=currentNode.getFromLinkEdgeList().get(i);
						TreeNode endNode=tempEdge.getEndNode();
						endNode.getToLinkEdgeList().remove(tempEdge);
					}
				}
				List<LinkEdge>list=new ArrayList<LinkEdge>();
				list.add(currentNode.getFromLinkEdgeList().get(index));
				System.out.println("剩余边:"+currentNode.getFromLinkEdgeList().get(index).getOverlapRate());
				currentNode.setFromLinkEdgeList(list);
			}
			if(currentNode.getChildNodes()==null||currentNode.getChildNodes().size()==0){
				continue;
			}
			for(TreeNode childNode:currentNode.getChildNodes()){
				nodeQueue.add(childNode);
			}
		}
		//情况2：判断是否存在循环边(此时每个节点只多可能有一条出边)
		nodeQueue=new LinkedList<TreeNode>();
		nodeQueue.add(rootNodeOfPatternTree);
		currentNode=null;
		while(!nodeQueue.isEmpty()){
			//当前节点
			currentNode=nodeQueue.poll();
//			System.out.println(currentNode.id+" "+nodeQueue.size());
			TreeNode tempNode=currentNode;
			boolean flag=false;
			while(tempNode!=null){
				if(tempNode.getFromLinkEdgeList()==null||tempNode.getFromLinkEdgeList().size()==0){
					break;
				}
				tempNode=tempNode.getFromLinkEdgeList().get(0).getEndNode();//此时每个节点只可能有一条出边
				if(tempNode.equals(currentNode)){//有循环产生
					flag=true;
					break;
				}
			}
			if(flag){//有循环产生
				System.out.println("有循环边产生！");
				double minOverlapRate=99999999;
				TreeNode minOverlapRateStartNode=null;//记录overlapRate最小的边对应的startNode
				LinkEdge linkEdge=null;//记录overlapRate最小的边
				while(tempNode!=null){
					double overlapRate=tempNode.getFromLinkEdgeList().get(0).getOverlapRate();
					if(overlapRate>minOverlapRate){
						minOverlapRate=overlapRate;
						minOverlapRateStartNode=tempNode;
						linkEdge=minOverlapRateStartNode.getFromLinkEdgeList().get(0);
					}
					tempNode=tempNode.getFromLinkEdgeList().get(0).getEndNode();
					if(tempNode.equals(currentNode)){
						break;
					}
				}
				TreeNode overlapRateMinEndNode=minOverlapRateStartNode.getFromLinkEdgeList().get(0).getEndNode();
				minOverlapRateStartNode.getFromLinkEdgeList().remove(linkEdge);
				overlapRateMinEndNode.getToLinkEdgeList().remove(linkEdge);
			}
			if(currentNode.getChildNodes()==null||currentNode.getChildNodes().size()==0){
				continue;
			}
			for(TreeNode childNode:currentNode.getChildNodes()){
				nodeQueue.add(childNode);
			}
		}
	}
	/**
	//根据url重写规则选择匹配节点将url重写
	public static String rewriteUrl(String url){
		String resultUrlStr=null;
		Queue<TreeNode>nodeQueue=new LinkedList<TreeNode>();
		nodeQueue.add(rootNodeOfPatternTree);
		TreeNode currentNode=null;
		int flag=-1;
		while(!nodeQueue.isEmpty()){
			currentNode=nodeQueue.poll();
			if(currentNode.getChildNodes()!=null&&currentNode.getChildNodes().size()>0){
				for (TreeNode childNode : currentNode.getChildNodes()) {
					if(childNode!=null){
						nodeQueue.add(childNode);
					}
				}
			}
			//找到符合要求的节点
//			if(currentNode.match(url)){
			if(Pattern.matches(currentNode.getPatternOutputStr(),CalculateUtil.getSortedUrl(url))){//currentNode可能没有出边
				if((currentNode.getFromLinkEdgeList()==null||currentNode.getFromLinkEdgeList().size()==0)
						&&(currentNode.getToLinkEdgeList()==null||currentNode.getToLinkEdgeList().size()==0)){
					continue;
				}
				if((currentNode.getFromLinkEdgeList()==null||currentNode.getFromLinkEdgeList().size()==0)
						&&(currentNode.getToLinkEdgeList()!=null||currentNode.getToLinkEdgeList().size()>0)){
					flag=1;
					break;
				}
				while(true){
					TreeNode tempNode=currentNode.getFromLinkEdgeList().get(0).getEndNode();
					if(tempNode.getFromLinkEdgeList()!=null&&tempNode.getFromLinkEdgeList().size()>0){
						System.out.print(currentNode.id+" "+tempNode.id);
						currentNode=tempNode;
					}else{
						break;
					}
				}
				flag=0;
				break;
			}
		}
		String resultStr;
		if(flag==0){
//			System.out.println("size:"+currentNode.getUrlsList().size());
//			System.out.println("startNode pattern:"+currentNode.getPatternOutputStr());
//			System.out.println(currentNode.getFromLinkEdgeList().get(0).getPatternStr());
//			String resultStr=currentNode.getFromLinkEdgeList().get(0).rewrite(url);//一个节点可能有多个入边，所以不能这么做
			resultStr=currentNode.getFromLinkEdgeList().get(0).rewrite(url);
			System.out.println("原始url为："+url);
			System.out.println("改写后url为："+resultStr);
			return resultStr;
		}
		else if(flag==1){
			resultStr=currentNode.getToLinkEdgeList().get(0).rewrite(url);
			System.out.println("原始url为："+url);
			System.out.println("改写后url为："+resultStr);
			return resultStr;
		}
		return url;
	}*/
	public static void main(String arogs[]) throws IOException{
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		List<ProcessedUrl>urlsList=InputUtil.getUrlsFromUrlFile("E:/URL/url2.txt");
		System.out.println(df.format(new Date()));
		List<Cluster>clusterList=InputUtil.getClustersFromFile("E:/cluster.txt");
//		List<ProcessedUrl>urlsList=InputUtil.getUrlsFromUrlFile("E:/cluster1.txt");
		List<ProcessedUrl>urlsList=new ArrayList<ProcessedUrl>();
		for(Cluster cluster:clusterList){
//			if(cluster.getUrlList().size()<20){
//				continue;
//			}
			for(ProcessedUrl url:cluster.getUrlList()){
				urlsList.add(url);
			}
		}
		System.out.println("URL数目："+urlsList.size());
		System.out.println("读url文件:"+df.format(new Date()));
		List<String> keyDoneList=new ArrayList<String>();
		Fun fun=new Fun();
		rootNodeOfPatternTree=fun.buildPatternTree(urlsList, keyDoneList,null,null);		
		System.out.println("建树:"+df.format(new Date()));
		System.out.println("准备工作:"+df.format(new Date()));
		System.out.println("节点总数:"+TreeNode.nodeNum);
		prepareForRuleGeneration(); 
		candidateRuleGeneration(clusterList);
		System.out.println("产生规则:"+df.format(new Date()));
		checkNodeEdge();
		selectDeloyableRule();
		checkNodeEdge();
		saveNodePatternAndRewriteRule(rootNodeOfPatternTree);
		System.out.println("已写入");
		Set <String>rewriteUrlSet=new HashSet<String>();
		Set <String>urlSet=new HashSet<String>();
		int sum=0;
		for(Cluster cluster:clusterList){
//			if(cluster.getUrlList().size()<20){
//				continue;
//			}
			for(ProcessedUrl url:cluster.getUrlList()){
				urlSet.add(url.getUrlStr());
				String urlStr=url.getUrlStr();
//				rewriteUrlSet.add(rewriteUrl(urlStr));
				rewriteUrlSet.add(CalculateUtil.rewriteFromFile(urlStr));
			}
		}
		System.out.println("有规则对应的URL数目："+CalculateUtil.sum);
		System.out.println("初始大小："+urlSet.size());   
		System.out.println("重写后大小："+rewriteUrlSet.size());
		System.out.println("压缩数目："+(urlSet.size()-rewriteUrlSet.size()));
		System.out.println("压缩率："+rewriteUrlSet.size()*1.0/urlSet.size());
	}
	public static void checkNodeEdge(){
		Queue<TreeNode>nodeQueue=new LinkedList<TreeNode>();
		nodeQueue.add(rootNodeOfPatternTree);
		TreeNode currentNode=null;
		boolean flag=false;
		System.out.println("检查是否一个节点有多条出边：");
		int fromSize=0,toSize=0,urlsNum=0;
		while(!nodeQueue.isEmpty()){
			currentNode=nodeQueue.poll();
			if(currentNode.getChildNodes()!=null){
				for(TreeNode node:currentNode.getChildNodes()){
					nodeQueue.add(node);
				}
			}
			if(currentNode.getFromLinkEdgeList()!=null){
				int size=currentNode.getFromLinkEdgeList().size();
				fromSize+=size;
				urlsNum+=currentNode.getUrlsList().size();
				if(size>1){
//					System.out.println(currentNode.id+" "+size);
					flag=true;
				}
			}
			if(currentNode.getToLinkEdgeList()!=null){
				toSize+=currentNode.getToLinkEdgeList().size();
				urlsNum+=currentNode.getUrlsList().size();
			}
		}
		System.out.println("入边总数和出现总数分别为："+fromSize+"　"+toSize+" "+urlsNum);
		if(!flag){
			System.out.println("未发现有多条节点的出边！");
		}
	}
	public static void saveNodePatternAndRewriteRule(TreeNode root){
		try {
			FileWriter writer=new FileWriter("E://patternAndRules.txt");
			List<String>list=getLeafTreeNodesPatterns(rootNodeOfPatternTree);
			for(String str:list){
				writer.write(str+"\r\n");
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static List<String> getLeafTreeNodesPatterns(TreeNode rootNode){
		Queue<TreeNode>queue=new LinkedList<TreeNode>();
		queue.add(rootNode);
		TreeNode currentNode;
		List<String>list=new ArrayList<String>();
		while(!queue.isEmpty()){
			currentNode=queue.poll();
			if(currentNode.getChildNodes()!=null&&currentNode.getChildNodes().size()>0){
				for(TreeNode node:currentNode.getChildNodes()){
					queue.add(node);
				}
			}
			String patternStr="";
//			for(char c:currentNode.getPatternOutputStr().toCharArray()){
			for(char c:currentNode.getPatternStr().toCharArray()){
				if(c=='*'){
					patternStr+=".*";
				}else{
					patternStr+=c;
				}
			}
			if(currentNode.getFromLinkEdgeList()!=null&&currentNode.getFromLinkEdgeList().size()>0){
				list.add(patternStr+"$$"+currentNode.getFromLinkEdgeList().get(0).getPatternStr());
			}
			else if(currentNode.getToLinkEdgeList()!=null&&currentNode.getToLinkEdgeList().size()>0){
				list.add(patternStr+"$$"+currentNode.getToLinkEdgeList().get(0).getPatternStr());
			}
		}
		return list;
	}
}