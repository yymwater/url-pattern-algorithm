package com.flytek.urlpattern.tree;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



import org.apache.commons.collections.functors.ForClosure;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.xalan.xsltc.compiler.sym;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.log.Log;

import sun.security.util.Length;
import sun.util.logging.resources.logging;

import com.iflytek.bloomfilter.BloomFilterWrapper;
import com.iflytek.common.UrlWildcardSearcher;
import com.iflytek.parse.html.analyzers.Url;

/**
 * author:Yang Yiming 
 * 模式树中的树节点 
 * date:2014.05.23
 * **/
public class TreeNode {
	public static int nodeNum;
	private List<ProcessedUrl> urlsList;// 该节点中的url列表
	private int urlsType;//是否均为/?，是否均无/?，是否两者都有
	private String patternStr;// 该节点中urlList对应的正则表达式(带key)
	private String patternOutputStr;// 该节点中urlList对应的正则表达式(不带key),用于输出
//	private List<String> partPatternList;
	private TreeNode parentNode;// 父节点
	private String partitionKeyStr;// 该节点拓展子节点时用到的划分属性
	private String partitionKeyValueStr;// 父节点拓展该节点，该节点在对应划分属性上的值
	private List<TreeNode> childNodes;// 子节点列表
	private List<LinkEdge> toLinkEdgeList;// 描述泛化关系：入边列表
	private List<LinkEdge> fromLinkEdgeList;//描述泛化关系：出边列表
	//private long supportNum;// 默认为url列表的大小 暂未使用
	private String protocolStr;// 协议名 暂未处理
	private boolean childHasStarFlag;//表明从该节点划分子节点是否含有包含星号的划分
	public int id;
	public boolean equals(Object obj) {//粗略的判断方法
		TreeNode node=(TreeNode)obj;
		if(patternStr.equals(node.getPatternStr())){
			if(urlsList.size()==node.urlsList.size()){
				return true;
			}
		}
		return false;
	}

	final public String getPatternStr() {
		return patternStr;
	}

	final public void setPatternStr(String patternStr) {
		this.patternStr = patternStr;
	}

	final public String getPatternOutputStr() {
		return patternOutputStr;
	}

	final public void setPatternOutputStr(String patternOutputStr) {
		this.patternOutputStr = patternOutputStr;
	}

	final public List<ProcessedUrl> getUrlsList() {
		return urlsList;
	}

	final public void setUrlsList(List<ProcessedUrl> urlsList) {
		this.urlsList = urlsList;
	}


	final public String getPartitionKeyStr() {
		return partitionKeyStr;
	}

	final public void setPartitionKeyStr(String partitionKeyStr) {
		this.partitionKeyStr = partitionKeyStr;
	}

	final public TreeNode getParentNode() {
		return parentNode;
	}

	final public void setParentNode(TreeNode parentNode) {
		this.parentNode = parentNode;
	}

	final public List<TreeNode> getChildNodes() {
		return childNodes;
	}

	public void setChildNodes(List<TreeNode> childNodes) {
		this.childNodes = childNodes;
	}

	final public List<LinkEdge> getToLinkEdgeList() {
		return toLinkEdgeList;
	}
	
	final public void addToLinkEdge(LinkEdge linkEdge){
		if(toLinkEdgeList==null){
			toLinkEdgeList=new ArrayList<LinkEdge>();
		}
		toLinkEdgeList.add(linkEdge);
	}
	
	
	final public void setToLinkEdgeList(List<LinkEdge> toLinkEdgeList) {
		this.toLinkEdgeList = toLinkEdgeList;
	}

	final public void setFromLinkEdgeList(List<LinkEdge> fromLinkEdgeList) {
		this.fromLinkEdgeList = fromLinkEdgeList;
	}

	public void addFromLinkEdge(LinkEdge linkEdge){
		if(fromLinkEdgeList==null){
			fromLinkEdgeList=new ArrayList<LinkEdge>();
		}
		fromLinkEdgeList.add(linkEdge);
	}
	
	final public List<LinkEdge> getFromLinkEdgeList() {
		return fromLinkEdgeList;
	}

	final public boolean isChildHasStarFlag() {
		return childHasStarFlag;
	}

	final public void setChildHasStarFlag(boolean childHasStarFlag) {
		this.childHasStarFlag = childHasStarFlag;
	}
	public int checkUrlsType(){//均含有/？的话返回1 均不含有/?的话返回2 否则返回3
		boolean allFlag=true;
		for(ProcessedUrl url:urlsList){//均含有/?
			if(!url.getUrlStr().contains("/?")){
				allFlag=false;
				break;
			}
		}
		if(allFlag)
			return 1;
		allFlag=true;
		for(ProcessedUrl url:urlsList){//均不含有/?
			if(url.getUrlStr().contains("/?")){
				allFlag=false;
				break;
			}
		}
		if(allFlag)
			return 2;
		return 3;
	}
	public static void main(String argos[]){
		List<ProcessedUrl>list=new ArrayList<ProcessedUrl>();
//		list.add(new ProcessedUrl("http://m.zhigou.com/?p=242&sort=32"));
//		list.add(new ProcessedUrl("http://m.zhigou.com/?p=2&size=32"));
//		list.add(new ProcessedUrl("http://m.zhigou.com/btoread/cclothing/"));
//		list.add(new ProcessedUrl("http://m.zhigou.com/btoread/csports/czhaoming/"));
//		System.out.println(checkUrlsType(list));
	}
	public TreeNode(List<ProcessedUrl> urlsList, TreeNode parentNode,
			String partitionKeyValue) {
		System.out.println("----------------------------------------------");
		nodeNum++;
		id=Fun.nodeId;
		System.out.println("id:"+id+" size:"+urlsList.size());
		Fun.nodeId++;
		this.urlsList = urlsList;
		urlsType=checkUrlsType();
		this.parentNode = parentNode;
		this.partitionKeyValueStr = partitionKeyValue;
		if(partitionKeyValueStr!=null&&partitionKeyValueStr.startsWith("#")&&partitionKeyValueStr.endsWith("#")){
			partitionKeyValueStr=partitionKeyValueStr.substring(1,partitionKeyValueStr.length()-1)+"\\d";
		}
		this.protocolStr = "http:/";
		patternStr = getPatternWithKeys();
//		System.out.println(partPatternList.size());
//		Log.info("urlList size:"+urlsList.size());
		patternOutputStr = getPatternWithoutKeys(patternStr);
		if(parentNode!=null){
			System.out.println("parent id:"+parentNode.id+" "+parentNode.partitionKeyStr+" "+partitionKeyValue);
		}
		System.out.println("该节点"+urlsList.size()+"个url，模式串为 "+patternStr);
		System.out.println("该节点"+urlsList.size()+"个url，模式串为 "+protocolStr+patternOutputStr);
//		for(ProcessedUrl url:urlsList){
//			System.out.println(url.getUrlStr());
//		}
		//延迟实例化(节省空间)
		childNodes = null;
		toLinkEdgeList = null;
		fromLinkEdgeList = null;
	}

	// 添加孩子节点
	public void addChild(TreeNode node) {
		if (childNodes == null) {
			childNodes = new ArrayList<TreeNode>();
		}
		childNodes.add(node);
	}

	// 返回该节点某key值所对应的value的列表
	public List<String> getValueForOneKey(String keyStr) {
		return ProcessedUrl.getValuesInTheKey(keyStr, urlsList);
	}

	// 判断node1是否是node2的祖先节点
	public static boolean checkIsAncestor(TreeNode node1, TreeNode node2) {
		TreeNode node = node2;
		while (true) {
			if (node == node1) {
				return true;
			}
			node = node.getParentNode();
			if (node == null)
				return false;
		}
	}

	// 返回该节点的URLList对应的所有key值列表
	final public static List<String> getKeys(TreeNode node) {
		List<String> keysList = ProcessedUrl.getKeysFromUrls(node.getUrlsList());
		return keysList;
	}

	// 把key:value转换成只有value或者key=value的形式,用于getPatternWithKeys()函数和getPatternWithoutKeys()函数
	final public static String getStandardKeyValueStr(String keyStr, String valueStr,
			int flag,boolean flag1) {
		String resultStr = "";
		if (flag == 0) {
			if(keyStr.endsWith("#1\\d#")){
				resultStr += "\\." + valueStr;
			}else if(keyStr.endsWith("#2\\d#")){
				resultStr += "-" + valueStr;
			}else if(keyStr.endsWith("#3\\d#")){
				resultStr += "_" + valueStr;
			}else{
				resultStr += "/" + valueStr;
			}
		} else if (flag == 1) {
			if(flag1){
				resultStr += "\\?" + keyStr + "=" + valueStr;
			}
			else{//暂时未使用到
				resultStr += "/\\?" + keyStr + "=" + valueStr;				
			}
		} else {
			Pattern pattern=Pattern.compile("#\\d{1,2}#");
			if(pattern.matcher(keyStr).find()){
				System.out.println("---");
				if(keyStr.endsWith("#11#")||keyStr.endsWith("#12#")||keyStr.endsWith("#13#")||keyStr.endsWith("#14#")||keyStr.endsWith("#15#")||keyStr.endsWith("#16#")||keyStr.endsWith("#17#")){
					resultStr += "\\." + valueStr;
				}else if(keyStr.endsWith("#21#")||keyStr.endsWith("#22#")||keyStr.endsWith("#23#")||keyStr.endsWith("#24#")||keyStr.endsWith("#25#")||keyStr.endsWith("#26#")||keyStr.endsWith("#27#")){
					resultStr += "-" + valueStr;
				}else if(keyStr.endsWith("#31#")||keyStr.endsWith("#22#")||keyStr.endsWith("#33#")||keyStr.endsWith("#34#")||keyStr.endsWith("#35#")||keyStr.endsWith("#36#")||keyStr.endsWith("#37#")){
					resultStr += "_" + valueStr;
				}else{
					resultStr += "/" + valueStr;
				}
			}
			else{
				resultStr += "&" + keyStr + "=" + valueStr;
			}
		}
//		System.out.println(keyStr+" "+valueStr+" "+flag);
//		System.out.println("#"+resultStr);
		return resultStr;
	}

	// 用于输出pattern(考虑？和&)
	final public static int checkFlag(String previousKeyStr, String keyStr) {
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

	// 获取本节点带keys的pattern(前提是先执行getPatternWithKeys)
	final public String getPatternWithoutKeys(String patternStr) {
		String resultStr = "";
		String part[] = patternStr.split("/");
		String keysStr[] = new String[part.length];
		String valuesStr[] = new String[part.length];
		for (int i = 0; i < part.length; i++) {
			try{
				keysStr[i] = part[i].split(":")[0];
				valuesStr[i] = part[i].split(":")[1];
			}catch(Exception e){
				Log.info("#urlsList:"+urlsList.size());
				for(ProcessedUrl url:urlsList){
					Log.info("get Pattern without key："+url.getUrlStr());
				}
				Log.info("出错#："+"length:"+part.length+" "+patternStr);
				Log.info(part[i]);
			}
		}
		int flag;
		for (int i = 0; i < part.length; i++) {
			//控制内容
			String valueStr="";
			if(valuesStr[i].contains("*")){
				List<String>valueList=ProcessedUrl.getValuesInTheKey(
					keysStr[i], urlsList);
				valueStr=checkNumOrLetterStr(valueList);
			}else{
				valueStr=valuesStr[i];
			}
			if (i > 0){
				flag = checkFlag(keysStr[i - 1], keysStr[i]);
			} else {
				flag = checkFlag(null, keysStr[i]);
			}
			//控制格式
			if(urlsType==2||urlsType==3)
				resultStr += getStandardKeyValueStr(keysStr[i], valueStr, flag,true);
			else if(urlsType==1)
				resultStr += getStandardKeyValueStr(keysStr[i], valueStr, flag,false);
//			System.out.println(":"+resultStr);
		}
//		if(resultStr.contains("?"))
			return resultStr;
//		else {
//			return resultStr+"$";
//		}
	}
	public static String checkNumOrLetterStr(List<String>valueStr){//针对*的情况
		boolean flag=true;
		Pattern pattern=Pattern.compile("^\\d*$");//所有属性值均为数字
		for(String str:valueStr){
			if(!pattern.matcher(str).find()){
				flag=false;
				break;
			}
		}
		if(flag){
			return "\\d*";
		}
		/**flag=true;
		boolean flag1=true;
		pattern=Pattern.compile("^[^\\d]+$");//所有属性值均为非数字
		for(String str:valueStr){
			if(flag1&&str.startsWith("#")&&str.endsWith("#")){
				flag1=false;
			}
			if(!pattern.matcher(str).find()){
				flag=false;
				break;
			}
		}
		if(flag&&flag1){
			return "[^\\d]+";
		}*/
		return ".*";//其他
	}
	// 获取本节点带keys的pattern
	final public String getPatternWithKeys() {
		// 针对根节点进行处理(根节点的父节点为null)
		String patternStr;
		if (parentNode == null) {
			patternStr = "";
			List<String> commonKeysList = CalculateUtil.getMostedKeysList(urlsList);
//			for(int i=0;i<commonKeysList.size();i++)
//				System.out.println(commonKeysList.get(i));
//			System.out.println("------------------");
			String keyStrArrays[] = CalculateUtil.sortList(commonKeysList);
//			for(int i=0;i<keyStrArrays.length;i++)
//				System.out.println(keyStrArrays[i]);
			for (int i = 0; i < keyStrArrays.length; i++) {
				List<String> valueSet = ProcessedUrl.getValuesInTheKey(
						keyStrArrays[i], urlsList);
				if (valueSet.size() == 1) {// 如果该key只对应一个value,则直接在pattern中使用此value
//					patternStr += keyStrArrays[i] + ":" + valueSet.toArray()[0]
//							+ "/";
					String valueStr=(String)valueSet.toArray()[0];
					if(valueStr.startsWith("#")&&valueStr.endsWith("#")){						
						patternStr += keyStrArrays[i] + ":" + valueStr.substring(1,valueStr.length()-1)+ "\\d*/";
					}else{
						patternStr += keyStrArrays[i] + ":" + valueStr	+ "/";						
					}
				} else {// 如果该key对应多个value,则直接在pattern中使用*
					patternStr += keyStrArrays[i] + ":*/";
				}
			}
			return patternStr;
		}
		List<KeyValuePair> pairs = CalculateUtil.getKeyValuePairList(parentNode.getPatternStr());
		// 得到该节点的所有共有key，明显包括父节点pattern中的key，新加的key后用*表示
//		List<String> commonKeyList = ProcessedUrl.getCommonKeysFromUrls(urlsList,null);
		List<String> commonKeyList = CalculateUtil.getMostedKeysList(urlsList);
		String commonKeyArr[] = CalculateUtil.sortList(commonKeyList);
		patternStr = "";
		for (int i = 0; i < commonKeyArr.length; i++) {
			boolean inParentPatternFlag = true;
			String value = null;
			for (KeyValuePair pair : pairs) {
				if (pair.getKeyStr().equals(commonKeyArr[i])) {
					inParentPatternFlag = false;
					value = pair.getValueStr();
					break;
				}
			}
//			System.out.println(commonKeyArr[i]+" "+ProcessedUrl.getValuesInTheKey(
//						commonKeyArr[i], urlsList).size());
			if (!inParentPatternFlag) {// 该key在父节点pattern中出现
				List<String> valueSet = ProcessedUrl.getValuesInTheKey(
						commonKeyArr[i], urlsList);
				if (valueSet.size() == 1) {// 如果该key只对应一个value,则直接在pattern中体现出来
					String valueStr=(String)valueSet.toArray()[0];
					if(valueStr.startsWith("#")&&valueStr.endsWith("#")){	
						List<String>valueSet1=ProcessedUrl.getValuesInTheKey1(commonKeyArr[i], urlsList);
						if(valueSet1.size()>1)//该判断为了识别出字母加数字串格式时数字串只有一个值的情况(else中)
							patternStr += commonKeyArr[i] + ":" + valueStr.substring(1,valueStr.length()-1)+ "\\d*/";
						else{
							valueStr=(String)valueSet1.toArray()[0];
							valueStr=valueStr.replaceAll("\\$","");
							patternStr += commonKeyArr[i] + ":" + valueStr.substring(1,valueStr.length()-1)+ "/";
						}
					}else{
						patternStr += commonKeyArr[i] + ":" + valueStr	+ "/";						
					}
				}
				else{
//					String mostedValue=ProcessedUrl.getMostedValuesInTheKey2(commonKeyArr[i], urlsList);
//					if(mostedValue==null)
						patternStr += commonKeyArr[i] + ":*/";
//					else
//						patternStr += commonKeyArr[i] + ":"+mostedValue+"/";
				}
			} else {// 该key不在父节点pattern中出现
				List<String> valueSet = ProcessedUrl.getValuesInTheKey(
						commonKeyArr[i], urlsList);
				if (valueSet.size() == 1) {// 如果该key只对应一个value,则直接在pattern中体现出来
					String valueStr=(String)valueSet.toArray()[0];
					if(valueStr.startsWith("#")&&valueStr.endsWith("#")){						
						List<String>valueSet1=ProcessedUrl.getValuesInTheKey1(commonKeyArr[i], urlsList);
						if(valueSet1.size()>1)//该判断为了识别出字母加数字串格式时数字串只有一个值的情况 (else中)
							patternStr += commonKeyArr[i] + ":" + valueStr.substring(1,valueStr.length()-1)+ "\\d*/";
						else{
							valueStr=(String)valueSet1.toArray()[0];
							valueStr=valueStr.replaceAll("\\$","");
							patternStr += commonKeyArr[i] + ":" + valueStr.substring(1,valueStr.length()-1)+ "/";
						}
					}else{
						patternStr += commonKeyArr[i] + ":" + valueStr	+ "/";						
					}
				} else {
//					String mostedValue=ProcessedUrl.getMostedValuesInTheKey2(commonKeyArr[i], urlsList);
//					if(mostedValue==null)
						patternStr += commonKeyArr[i] + ":*/";
//					else
//						patternStr += commonKeyArr[i] + ":"+mostedValue+"/";
				}
			}
		}
		if(parentNode.getPartitionKeyStr()==null){
			String part[]=patternStr.split("/");
			int sum=0;
			for(int i=0;i<part.length;i++){
				String temp[]=part[i].split(":");
				if(!temp[0].startsWith("path")){
					sum=2;break;
				}
				if(!temp[1].equals("*")){//temp[0].startsWith("path")&&
					sum++;
				}
			}
			return patternStr;
		}
		String resultPatternStr = null;
		int index = patternStr.indexOf(parentNode.getPartitionKeyStr() + ":");
//		System.out.println("patternStr:"+patternStr);
		String tempStr = patternStr.substring(0, index);
		
		//再次判断该属性是否只有单个值
		boolean flag=false;
		List<String> valueSet = ProcessedUrl.getValuesInTheKey(
				parentNode.getPartitionKeyStr(), urlsList);
		if (valueSet.size() == 1) {// 如果该key只对应一个value,则直接在pattern中体现出来
			String valueStr=(String)valueSet.toArray()[0];
			if(valueStr.startsWith("#")&&valueStr.endsWith("#")){						
				flag=true;
				List<String>valueSet1=ProcessedUrl.getValuesInTheKey1(parentNode.getPartitionKeyStr(), urlsList);
				if(valueSet1.size()>1)//该判断为了识别出字母加数字串格式时数字串只有一个值的情况 (else中)
					resultPatternStr = tempStr + parentNode.getPartitionKeyStr() + ":" + valueStr.substring(1,valueStr.length()-1)+ "\\d*";
				else{
					valueStr=(String)valueSet1.toArray()[0];
					valueStr=valueStr.replaceAll("\\$","");
					resultPatternStr = tempStr + parentNode.getPartitionKeyStr() + ":" + valueStr.substring(1,valueStr.length()-1);
				}
//				resultPatternStr = tempStr + parentNode.getPartitionKeyStr() + ":"
//						+ valueStr.substring(1,valueStr.length()-1)+ "\\d";
			}
		}
		if(!flag)
			resultPatternStr = tempStr + parentNode.getPartitionKeyStr() + ":"
				+ partitionKeyValueStr;
//		System.out.println(partitionKeyValueStr);
		tempStr = patternStr.substring(index, patternStr.length());
		index = tempStr.indexOf("/");
		resultPatternStr += tempStr.substring(index);
//		System.out.println("result:"+resultPatternStr);
		//添加到partPatternList
		String part[]=resultPatternStr.split("/");
		int sum=0; 
		for(int i=0;i<part.length;i++){
			String temp[]=part[i].split(":");
			if(!temp[0].startsWith("path")){
				sum=2;break;
			}
			if(!temp[1].equals("*")){//temp[0].startsWith("path")&&
				sum++;
			}
		}
		if(sum==1){
			return resultPatternStr;
		}
		return resultPatternStr;
	}

	// 该节点所含模式是否匹配该url
	
	final public boolean matchByRegex(String url) throws MalformedURLException {
		String patternStr="";
		for(char c:patternOutputStr.toCharArray()){
			if(c=='*'){
				patternStr+=".*";
			}else{
				patternStr+=c;
			}
		}
		Pattern p = Pattern.compile(patternStr);		
		String resultStr = CalculateUtil.getSortedUrl(url);
		Matcher matcher = p.matcher(resultStr);
        return matcher.find();
	}
	
	final public boolean match(String url) {
		List<String> patternKeysList = new ArrayList<String>();//模式串中的key
		List<String> patternValuesList = new ArrayList<String>();//模式串中的value(和KeyList顺序一致)
		String s[] = patternStr.split("/");
		for (int i = 0; i < s.length; i++){
			if (s[i] == null || s[i].equals(""))
				continue;
			patternKeysList.add(s[i].split(":")[0]);
			patternValuesList.add(s[i].split(":")[1]);
		}
		ProcessedUrl processedUrl = new ProcessedUrl(url);
		//对匹配串中的key进行遍历处理
		for (KeyValuePair pair : processedUrl.getKeyValuePairs()) {
			String keyStr = pair.getKeyStr();// 匹配串中该pair中的key
			String valueStr = pair.getValueStr();// 匹配串中该pair中的value
			int index = patternKeysList.indexOf(keyStr);                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
			if (index != -1) {//模式串中有该key
				String patternValueStr = patternValuesList.get(index);// 模式串中key对应的value值
				if (!patternValueStr.equals("*")
						&& !valueStr.equals(patternValueStr)) {
					return false;
				}
			}
			//模式串中没有该key则不考虑
		}  
		//对在模式串中出现的key但不在匹配串中出现的key进行处理，该种情况下返回false
		for(int i=0;i<s.length;i++){
			String keyStr=patternKeysList.get(i);
			String valueStr=processedUrl.getValueOfTheKeyInOneUrl(keyStr);
			if(valueStr==null){
				return false;
			}
		}
		return true;
	}
	final public boolean match(ProcessedUrl url) {
		List<String> patternKeysList = new ArrayList<String>();//模式串中的key
		List<String> patternValuesList = new ArrayList<String>();//模式串中的value(和KeyList顺序一致)
		String s[] = patternStr.split("/");
		for (int i = 0; i < s.length; i++){
			if (s[i] == null || s[i].equals(""))
				continue;
			patternKeysList.add(s[i].split(":")[0]);
			patternValuesList.add(s[i].split(":")[1]);
		}
		//对匹配串中的key进行遍历处理
		for (KeyValuePair pair : url.getKeyValuePairs()) {
			String keyStr = pair.getKeyStr();// 匹配串中该pair中的key
			String valueStr = pair.getValueStr();// 匹配串中该pair中的value
			int index = patternKeysList.indexOf(keyStr);                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
			if (index != -1) {//模式串中有该key
				String patternValueStr = patternValuesList.get(index);// 模式串中key对应的value值
				if (!patternValueStr.equals("*")
						&& !valueStr.equals(patternValueStr)) {
					return false;
				}
			}
			//模式串中没有该key则不考虑
		}  
		//对在模式串中出现的key但不在匹配串中出现的key进行处理，该种情况下返回false
		for(int i=0;i<s.length;i++){
			String keyStr=patternKeysList.get(i);
			String valueStr=url.getValueOfTheKeyInOneUrl(keyStr);
			if(valueStr==null){
				return false;
			}
		}
		return true;
	}
	// 判断两个节点是否相似(之前做的是扫描所有url)
	final public static double checkTwoNodeSimilarOrNot(List<Cluster> clusterList,
			TreeNode node1, TreeNode node2) {
		double overlap = 0;//重复率 该值大于一定阈值，即可认为两节点是duplicate node
		double num = 0;// 分子
		boolean flag;
		for (Cluster cluster : clusterList) {//遍历所有cluster
			for (ProcessedUrl processedUrl : cluster.getUrlList()) {//遍历该cluster的所有url
				//该url在node1中或者node2中则num++
				flag = false;
				for (ProcessedUrl tempUrl : node1.getUrlsList()) {
					if (processedUrl.equals(tempUrl)) {
						flag = true;
						break;
					}
				}
				if (flag) {
					num++;
					continue;
				}
				for (ProcessedUrl tempUrl : node2.getUrlsList()) {
					if (processedUrl.equals(tempUrl)) {
						flag = true;
						break;
					}
				}
				if (flag) {
					num++;
				}
			}
		}
		overlap = num/(node1.getUrlsList().size() + node2.getUrlsList().size());
		return overlap;
	}
	//在某个cluster基础上判断两个node是否相似
	public static double checkTwoNodeSimilarOrNot1(Cluster cluster,
			TreeNode node1, TreeNode node2) {
		double overlap = 0;// 重复率 该值大于一定阈值，即可认为两节点是duplicate node
		double num = 0;// 分子
		boolean flag;
		for (ProcessedUrl processedUrl : cluster.getUrlList()) {// 遍历该cluster的所有url
			// 该url在node1中或者node2中则num++
			flag = false;
			for (ProcessedUrl tempUrl : node1.getUrlsList()) {
				if (processedUrl.equals(tempUrl)) {
					flag = true;
					break;
				}
			}
			if (flag) {
				num++;
				continue;
			}
			for (ProcessedUrl tempUrl : node2.getUrlsList()) {
				if (processedUrl.equals(tempUrl)) {
					flag = true;
					break;
				}
			}
			if (flag){
				num++;
			}
		}
		overlap = num/(node1.getUrlsList().size() + node2.getUrlsList().size());
		return overlap;
	}
	public boolean hasUrl(ProcessedUrl url){
		for(ProcessedUrl processedUrl:urlsList){
			if(processedUrl.equals(url))
				return true;
		}
		return false;
	}
	// 遍历树上所有节点然后返回和cluster相关的节点列表(复杂度过高，已弃用)
	public static List<TreeNode> getRelevantNodes(TreeNode rootNode,
			Cluster cluster) {
		List<TreeNode> nodes = new ArrayList<TreeNode>();
		Queue<TreeNode> nodeQueue = new LinkedList<TreeNode>();
		TreeNode tempNode;
		nodeQueue.add(rootNode);
		while (!nodeQueue.isEmpty()) {
			tempNode = nodeQueue.poll();
			boolean flag = false;
//			for (ProcessedUrl processedUrl: tempNode.getUrlsList()) {
//				if(cluster.hasUrl(processedUrl)){
//					flag=true;
//					break;
//				}
//			}
			for(ProcessedUrl processedUrl:cluster.urlList){
				if(tempNode.hasUrl(processedUrl)){
					flag=true;
					break;
				}
			}
			if (flag) {
				nodes.add(tempNode);
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
		return nodes;
	}
	//返回和cluster相关的节点列表
	public static List<TreeNode>getRelevantNodes(Cluster cluster){
		Set<TreeNode>set=new HashSet<TreeNode>();
		for(ProcessedUrl url:cluster.getUrlList()){
			for(TreeNode treeNode:url.getBelongToNodeList()){				
				set.add(treeNode);
			}
		}
		Iterator<TreeNode>iterator=set.iterator();
		List<TreeNode>list=new ArrayList<TreeNode>();
		while(iterator.hasNext()){
			list.add(iterator.next());
		}
		return list;
	}
	//返回根节点所代表的树中包含的所有相似节点对
	public static List<TreeNodePair> identifyDuplicateNodes(TreeNode rootNode,
			List<Cluster> clusterList) {
		List<List<TreeNode>>indexList=new ArrayList<List<TreeNode>>();//每一个cluster对应一个List<TreeNode>
		for(int i=0;i<clusterList.size();i++){//得到与每个cluster相关的所有node
			List<TreeNode>nodeList=getRelevantNodes(clusterList.get(i));
//			System.out.println("第"+i+"个cluster，url数目为"+clusterList.get(i).getUrlList().size()+",相关节点数为"+nodeList.size());
			indexList.add(nodeList);
		}
		List<TreeNodePair>pairs=new ArrayList<TreeNodePair>();
		for(int i=0;i<indexList.size();i++){//相当于遍历每个cluster
			int length=indexList.get(i).size();
//			System.out.println("处理第"+i+"个cluster，大小为"+length);
			for(int j=0;j<length;j++){//查询和该cluster相关的node集合组成的所有node对，如果某node对相似度较大则添加到pair中
				for(int k=j+1;k<length;k++){
					/**以下为剪枝*/
					double temp=((double)clusterList.get(i).getUrlList().size())/
						(indexList.get(i).get(j).getUrlsList().size()+indexList.get(i).get(k).getUrlsList().size());
					if(temp<0.5)
						continue;
					double overlapRate=checkTwoNodeSimilarOrNot1(clusterList.get(i),indexList.get(i).get(j),indexList.get(i).get(k));
					if(overlapRate<0.5){//重合率小于一定阈值则不考虑
						continue;
					}
					//使得URL包含key少的node指向URL包含key多的node
					int keyNum1=ProcessedUrl.getKeysFromUrls(indexList.get(i).get(j).getUrlsList()).size();
					int keyNum2=ProcessedUrl.getKeysFromUrls(indexList.get(i).get(k).getUrlsList()).size();
//					System.out.println("Key个数比较："+keyNum1+" "+keyNum2);
					TreeNodePair pair=null;
					if(keyNum1<keyNum2){ 
						pair=new TreeNodePair(indexList.get(i).get(j),indexList.get(i).get(k),overlapRate);
					}
					else if(keyNum1>keyNum2){
						pair=new TreeNodePair(indexList.get(i).get(k),indexList.get(i).get(j),overlapRate);
					}else{
						if(indexList.get(i).get(j).id<indexList.get(i).get(k).id){
							pair=new TreeNodePair(indexList.get(i).get(j),indexList.get(i).get(k),overlapRate);						
						}
						else{							
							pair=new TreeNodePair(indexList.get(i).get(k),indexList.get(i).get(j),overlapRate);						
						}
					}
					pairs.add(pair);
				}
			}
		}
		return pairs;
	}

	// 返回两节点的不同key中共同value的比例
	public static double rateOfCommonValues(TreeNode node1, TreeNode node2,
			String key1, String key2) {
		List<String> valueList1 = ProcessedUrl.getValuesInTheKey(key1,
				node1.getUrlsList());
		List<String> valueList2 = ProcessedUrl.getValuesInTheKey(key2,
				node2.getUrlsList());
		double sum = 0;//相同值的数目
		for (String valueStr : valueList1) {
			if (valueList2.indexOf(valueStr) != -1) {
				sum++;
			}
		}
		return sum/(node1.getUrlsList().size() + node2.getUrlsList().size() - sum);
	}

	// 返回两个相似节点的key-key对
	public static List<KeyPair> findKeyToKeyMapping(TreeNode s, TreeNode t) {
		List<KeyPair> list = new ArrayList<KeyPair>();
		List<String> keys1List = ProcessedUrl.getKeysFromUrls(s.getUrlsList());
		List<String> keys2List = ProcessedUrl.getKeysFromUrls(t.getUrlsList());
		for (String key2 : keys2List) {//t节点
			double maxRateOfCommonValue = -1;
			String key1 = null;// 存放于key2最相似的key
			for (String key : keys1List) {//s节点
				double temp = rateOfCommonValues(s, t, key, key2);
				if (temp >= maxRateOfCommonValue) {
					maxRateOfCommonValue = temp;
					key1 = key;
				}
			}
			if (key1 != null && maxRateOfCommonValue >=0.5)
				list.add(new KeyPair(key1, key2));
		}
		return list;
	}

	// 针对有很大比例URL重合的identified node
	public static void rewrite(TreeNodePair pair) {
		TreeNode node1=pair.getSourceNode();
		TreeNode node2=pair.getTargetNode();
		List<KeyPair>keyPairs=findKeyToKeyMapping(node1, node2);//有关的
		HashMap<String,Boolean>keyMap=new HashMap<String,Boolean>();
		String patternStr="";
		for(KeyPair keyPair:keyPairs){//replace操作
			patternStr+=keyPair.getKey2()+":"+keyPair.getKey1()+"/";
			if(keyMap.get(keyPair.getKey2())==null){
				keyMap.put(keyPair.getKey2(),true);
			}
		}
		//这里必须是针对Key2List
		List<String>key2List=getKeys(node2);
		for(String key:key2List){
			if(keyMap.get(key)==null){
				List<String>list=ProcessedUrl.getValuesInTheKey(key,node2.getUrlsList());
				if(list.size()==1){//keep操作 #作为标记，后面跟该key对应的唯一值
					patternStr+=key+":#"+list.get(0)+"/";			
				}else {//ignore操作 *作为标记，后面跟一个任意的值
					patternStr+=key+":*"+list.get(0)+"/";
				}
			}
		}
		LinkEdge linkEdge=new LinkEdge(node1,node2);
		linkEdge.setPatternStr(patternStr);
		linkEdge.setMapKeyNum(keyPairs.size());
		linkEdge.setOverlapRate(pair.getOverlapRate());;
		node1.addFromLinkEdge(linkEdge);
		node2.addToLinkEdge(linkEdge);
	}
}