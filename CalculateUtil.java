package com.flytek.urlpattern.tree;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * author:Yang Yiming 
 * 用于计算的工具类 
 * date:2014.05.23
 * **/
public class CalculateUtil {
	private static String value[];
	private static int times[];
//	public static List<String>keyListFromUrl;//有序存放一个host下面所有非path开头的key。
	public static void swap(int i, int j) {
		int timesTemp = times[i];
		times[i] = times[j];
		times[j] = timesTemp;
		String valueTemp = value[i];
		value[i] = value[j];
		value[j] = valueTemp;
	}

	// 冒泡：从小到大的顺序
	public static boolean bubbleSort() {
		int length = value.length;
		boolean flag = true;
		;
		for (int i = 1; i < length; i++) {
			if (times[i] != times[0]) {
				flag = false;
				break;
			}
		}
		if (flag) {
			return flag;
		}
		for (int i = 0; i < length - 1; i++) {
			for (int j = 0; j < length - i - 1; j++) {
				if (times[j] > times[j + 1]) {
					swap(j, j + 1);
				}
			}
		}
		return false;
	}

	// 快排：从小到大的顺序
	public static void quickSort(int left, int right) {
		if (left >= right)
			return;
		int i = left, j = right, vot = times[i];
		String vot1 = value[i];
		while (i < j) {
			while (times[j] >= vot && i < j) {
				j--;
			}
			if (times[j] < vot && i < j) {
				swap(i, j);
				i++;
			}
			while (times[i] <= vot && i < j) {
				i++;
			}
			if (times[i] > vot && i < j) {
				swap(i, j);
				j--;
			}
		}
		times[i] = vot;
		value[i] = vot1;
		i++;
		j--;
		quickSort(left, j);
		quickSort(i, right);
	}

	public static HashMap<String, Boolean> judgeValuesIsTrivialOrNot(
			HashMap<String, Integer> valueAndTimesMap) {
		// 排序（从小到大）
		int length = valueAndTimesMap.keySet().size();
		// System.out.println("length:"+length);
		value = new String[length];
		times = new int[length];
		int index = 0;
		Iterator<String> iterator = valueAndTimesMap.keySet().iterator();
		while (iterator.hasNext()) {
			String valueTemp = iterator.next();
			value[index] = valueTemp;
			times[index] = valueAndTimesMap.get(valueTemp);
			index++;
		}
		// quickSort(0, length-1);
		boolean isSameArray = bubbleSort();// 替代上行代码
		HashMap<String, Boolean> valueIsTrivialOrNotMap = new HashMap<String, Boolean>();
		// 特殊情况判断 length=1
		if (length == 1) {
			valueIsTrivialOrNotMap.put(value[0], false);
			return valueIsTrivialOrNotMap;
		}
		if (isSameArray) {
			for (int i = 0; i < length; i++) {
				valueIsTrivialOrNotMap.put(value[i], true);
			}
			return valueIsTrivialOrNotMap;
		}
		// 特殊情况判断 length=2
		int sum = 0;// times总和
		for (int i = 0; i < length; i++)
			sum += times[i];
		if (length == 2) {
			valueIsTrivialOrNotMap.put(value[0], true);
			if (times[1] >= times[0] * 9) {
				valueIsTrivialOrNotMap.put(value[1], false);
			} else {
				valueIsTrivialOrNotMap.put(value[1], true);
			}
			// if(times[0]*1.0/sum>0.1&&sum>=50){
			// valueIsTrivialOrNotMap.put(value[0],false);
			// }
			if (times[0] >= 50) {
				valueIsTrivialOrNotMap.put(value[0], false);
			}
			if (times[0] <= 30) {
				valueIsTrivialOrNotMap.put(value[0], true);
			}
			if (times[1] >= 50) {
				valueIsTrivialOrNotMap.put(value[1], false);
			}
			if (times[1] <= 30) {
				valueIsTrivialOrNotMap.put(value[1], true);
			}
			return valueIsTrivialOrNotMap;
		}
		// length>=3
		double max = -99999999;
		double diffArray[] = new double[length - 1];// 用于判断trivial，如果排序后相邻的所有的属性差值不大，则认为均为trivial
		index = 0;
		for (int i = 1; i < length; i++) {
			double logTemp = Math.log(times[i]) - Math.log(times[i - 1]);
			diffArray[i - 1] = logTemp;
			if (logTemp > max) {
				max = logTemp;
				index = i;
			}
		}
		// System.out.println();
		Arrays.sort(diffArray);
		// 如果对diffArray排序后发现相邻的属性差值不大，则认为所有value均为trivial
		/**
		 * if(diffArray[length-2]<=diffArray[length-3]*2){//length>=3 for(int
		 * i=0;i<length;i++){ valueIsTrivialOrNotMap.put(value[i],true); }
		 * return valueIsTrivialOrNotMap; }
		 **/
		for (int i = 0; i < index; i++) {
			valueIsTrivialOrNotMap.put(value[i], true);
		}
		for (int i = index; i < length; i++) {
			valueIsTrivialOrNotMap.put(value[i], false);
		}
		for (int i = 0; i < length; i++) {
			if (times[i] < 30) {
				valueIsTrivialOrNotMap.put(value[i], true);
			}
			if (times[i] > 50) {
				valueIsTrivialOrNotMap.put(value[i], false);
			} else if (sum > 500 && times[i] * 1.0 / sum > 0.1) {
				valueIsTrivialOrNotMap.put(value[i], false);
			} else if (sum > 200 && times[i] * 1.0 / sum > 0.15) {
				valueIsTrivialOrNotMap.put(value[i], false);
			} else if (sum > 100 && times[i] * 1.0 / sum > 0.2) {
				valueIsTrivialOrNotMap.put(value[i], false);
			} else if (sum > 50 && times[i] * 1.0 / sum > 0.3) {
				valueIsTrivialOrNotMap.put(value[i], false);
			}
		}
		return valueIsTrivialOrNotMap;
	}

	// 给定某一key对应的ValueAndTimesMap，返回熵
	public static double getEntropyFromValuesMap(
			HashMap<String, Integer> valueAndTimesMap, int size) {
		Iterator<String> iterator = valueAndTimesMap.keySet().iterator();
		String valueStr;
		double result = 0;
		iterator = valueAndTimesMap.keySet().iterator();
		while (iterator.hasNext()) {
			valueStr = iterator.next();
			double tmp = valueAndTimesMap.get(valueStr) / ((double) size);// 该value出现的频率
			result += (-tmp * (Math.log(tmp) / Math.log(2)));// 加上-Pi*log(Pi)
		}
		return result;
	}

	// 给定urlList为所有非done的key计算熵
	public static HashMap<String, Double> calculateEntropyForEachKey(
			List<ProcessedUrl> urlList, List<String> keyDoneList) {
		// System.out.println("size:"+urlList.size());
		// List<String>keyList=ProcessedUrl.getCommonKeysFromUrls(urlList,keyDoneList);
		List<String> keyList = getMostedKeysList(urlList, keyDoneList);
//		System.out.print("common keys size:" + keyList.size() + " ");
		HashMap<String, Double> keyAndEntropyMap = new HashMap<String, Double>();
//		for (String key : keyList) {
//			System.out.print(key + " ");
//		}
//		System.out.println();
		for (String key : keyList) {// 针对每一个key进行计算
			// 计算该key中所有value的出现次数
			HashMap<String, Integer> valueAndTimesMap = new HashMap<String, Integer>();
			for (ProcessedUrl url : urlList) {
				String valueStr = url.getValueOfTheKeyInOneUrl(key);
				if (valueStr == null)
					continue;
				if (valueStr.startsWith("#") && valueStr.endsWith("#")) {
					int index = valueStr.indexOf("$");
					valueStr = valueStr.substring(1, index);
				}
				if(Pattern.matches("\\d+",valueStr))
					valueStr="\\d*";
				if (valueAndTimesMap.get(valueStr) == null) {
					valueAndTimesMap.put(valueStr, 1);
				} else {
					int timesTemp = valueAndTimesMap.get(valueStr) + 1;
					valueAndTimesMap.put(valueStr, timesTemp);
				}
			}
			// for(String value:valueAndTimesMap.keySet()){
			// System.out.println(value+"出现次数："+valueAndTimesMap.get(value));
			// }
			double entropyTemp = getEntropyFromValuesMap(valueAndTimesMap,
					urlList.size());
			keyAndEntropyMap.put(key, entropyTemp);
//			System.out.println("key对应的熵： " + key + "　" + entropyTemp);
		}
		return keyAndEntropyMap;
	}
	
	//找出URL集合中所有值均为数字的key
	public static List<String>getAllKeysOnlyWithNumValues(List<ProcessedUrl> urlList){
		Set<String>keySet=new HashSet<String>();
		Set<String>keyWithNoNumValuesSet=new HashSet<String>();
		for(ProcessedUrl url:urlList){
			for(KeyValuePair pair:url.getKeyValuePairs()){
				keySet.add(pair.getKeyStr());
				if(!keyWithNoNumValuesSet.contains(pair.getKeyStr())){
					if(!Pattern.matches("\\d+",pair.getValueStr())){
						keyWithNoNumValuesSet.add(pair.getKeyStr());
					}
				}
			}
		}
		List<String>list=new ArrayList<String>();
		Iterator<String>iterator=keySet.iterator();
		String tempStr;
		while(iterator.hasNext()){
			tempStr=iterator.next();
			if(!keyWithNoNumValuesSet.contains(tempStr)){
				list.add(tempStr);
			}
		}
		return list;
	}
	public static String calculateAndSelectKeyForNoCommonKeys(
			List<ProcessedUrl> urlList, List<String> keyDoneList) {
		//把值均为数字的key加到keydoneList
//		List<String>keysWithOnlyNumValuesList=getAllKeysOnlyWithNumValues(urlList);
//		Iterator<String>iterator=keysWithOnlyNumValuesList.iterator();
//		while(iterator.hasNext()){
//			keyDoneList.add(iterator.next());
//		}
		// System.out.println("size:"+urlList.size());
		List<String> keyList = getMostedKeysList(urlList, keyDoneList);
		// System.out.println("Most keys size:"+keyList.size());
		double minEntropy = 99999999;
		String minEntropyKeyStr = null;
		for (String key : keyList) {// 针对每一个key进行计算
			// 计算该key中所有value的出现次数
			HashMap<String, Integer> valueAndTimesMap = new HashMap<String, Integer>();
			for (ProcessedUrl url : urlList) {
				String valueStr = url.getValueOfTheKeyInOneUrl(key);
				if (valueStr == null)
					continue;
				if (valueAndTimesMap.get(valueStr) == null) {
					valueAndTimesMap.put(valueStr, 1);
				} else {
					int timesTemp = valueAndTimesMap.get(valueStr) + 1;
					valueAndTimesMap.put(valueStr, timesTemp);
				}
			}
			// for(String value:valueAndTimesMap.keySet()){
			// System.out.println(value+"出现次数："+valueAndTimesMap.get(value));
			// }
			double entropyTemp = getEntropyFromValuesMap(valueAndTimesMap,
					urlList.size());
			if (entropyTemp < minEntropy) {
				minEntropy = entropyTemp;
				minEntropyKeyStr = key;
			}
			valueAndTimesMap = null;// ##
			// System.out.println("key对应的熵： "+key+"　"+entropyTemp);
		}
		// System.out.println("最终选择的key为："+minEntropyKeyStr);
		return minEntropyKeyStr;
	}

	// 输入模式串和匹配串，判断是否匹配
	public static boolean checkMatchOrNot(String patternStr, String matchStr) {
		Pattern pattern = Pattern.compile(patternStr);
		Matcher matcher = pattern.matcher(matchStr);
		return matcher.matches();
	}

	// 从一个pattern中抽取keyValuePair input: k1:v1/k2:v2/ outpout:(k1,v1)(k2,v2)
	public static List<KeyValuePair> getKeyValuePairList(String patternStr) {
		List<KeyValuePair> keyList = new ArrayList<KeyValuePair>();
		Pattern pattern = Pattern.compile(".*?/");
		Matcher matcher = pattern.matcher(patternStr);
		while (matcher.find()) {
			String keyStr = matcher.group().replaceAll("/", "");
			String s[] = keyStr.split(":");// 注意这里要删去/
			keyList.add(new KeyValuePair(s[0], s[1]));
		}
		return keyList;
	}
	
//	//从输入的urlList中自动学习非path开头的key的顺序，并有序存放在Calculate.keyListFromUrl静态对象中
//	public static void learnToRankNoPathKeys(List<ProcessedUrl>urlList){
//		int tempNum=0,maxNoPathKeyNum=0;
//		ProcessedUrl tempUrl=null;
//		for(ProcessedUrl url:urlList){
//			tempNum=0;
//			for(KeyValuePair pair:url.getKeyValuePairs()){
//				if(!checkKeystartWithPath(pair.getKeyStr())){
//					tempNum++;
//				}
//			}
//			if(tempNum>maxNoPathKeyNum){
//				tempNum=maxNoPathKeyNum;
//				tempUrl=url;
//			}
//		}
//		if(tempUrl!=null){
//			List<String>keyList=new ArrayList<String>();
//			for(KeyValuePair pair:tempUrl.getKeyValuePairs()){
//				if(!checkKeystartWithPath(pair.getKeyStr())){
//					keyList.add(pair.getKeyStr());
//				}
//			}
//			keyListFromUrl=keyList;
//		}
//	}
	
	public static boolean checkKeystartWithPath(String keyStr) {
		if (!keyStr.startsWith("path"))
			return false;
		try {
			String suffixStr = keyStr.substring(4);
			if(Pattern.matches("\\d+|\\d+#\\d#",suffixStr)){
				return true;
			}else{
				return false;
			}
		} catch (Exception e) {
			return false;
		}
	}
	
	// 对keys的列表进行字典排序，返回一个有序数组，用于在生成URL pattern时对key的字符串数组排序
	// 和普通排序不同的是：加入了对path的特判
	public static String[] sortList(List<String> list) {
		String str[] = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			str[i] = list.get(i);
		}
		Arrays.sort(str, new Comparator<String>() {
			public int compare(String o1, String o2) {
				String s1 = (String) o1;
				String s2 = (String) o2;
				if (checkKeystartWithPath(s1) && !checkKeystartWithPath(s2)) {
					return -1;
				} else if (checkKeystartWithPath(s2)
						&& !checkKeystartWithPath(s1)) {
					return 1;
				} else {
					return s1.compareTo(s2);
				}
			}
		});
		return str;
	}
	/**
	 * 用于对原始url根据key的顺序进行重排序，以path开头的key排在非path开头的key前面，非path开头的key按字典序排列
	 * 在匹配url时使用
	 * */
	public static String getSortedUrl(String url) throws MalformedURLException {
		String resultStr = "http:/";
		ProcessedUrl1 processedUrl = new ProcessedUrl1(url);
		List<String> keysList = new ArrayList<String>();
		for (KeyValuePair pair : processedUrl.getKeyValuePairs()) {
			keysList.add(pair.getKeyStr());
		}
		String keyStrArrays[] = CalculateUtil.sortList(keysList);//对url中包含的key进行排序
		int flag;
		for (int i = 0; i < keyStrArrays.length; i++) {
			if (i > 0) {
				flag = TreeNode.checkFlag(keyStrArrays[i - 1], keyStrArrays[i]);
			} else {
				flag = TreeNode.checkFlag(null, keyStrArrays[i]);
			}
			//将key对应的value值按key的顺序拼接起来
			resultStr+=getStandardKeyValueStr(keyStrArrays[i],processedUrl.getValueOfTheKeyInOneUrl(keyStrArrays[i]),flag);
		}
		return resultStr;
	}

	public static List<String> getMostedKeysList(
			List<ProcessedUrl> processedUrlsList, List<String> noUseKeysList) {
		double urlSize = processedUrlsList.size();
		HashMap<String, Integer> keyAndTimesMap = new HashMap<String, Integer>();
		for (ProcessedUrl url : processedUrlsList) {
			for (KeyValuePair pair : url.getKeyValuePairs()) {
				if (noUseKeysList.indexOf(pair.getKeyStr()) != -1) {
					continue;
				}
				if (keyAndTimesMap.get(pair.getKeyStr()) == null) {
					keyAndTimesMap.put(pair.getKeyStr(), 1);
				} else {
					int times = keyAndTimesMap.get(pair.getKeyStr()) + 1;
					keyAndTimesMap.put(pair.getKeyStr(), times);
				}
			}
		}
		List<String> keyList = new ArrayList<String>();
		Iterator<String> iterator = keyAndTimesMap.keySet().iterator();
		while (iterator.hasNext()) {
			String keyStr = iterator.next();
			int times = keyAndTimesMap.get(keyStr);
//			 System.out.println("无common Key时："+keyStr+" "+times);
			if (times * 1.0 / urlSize > 0.9) {
				keyList.add(keyStr);
			}
		}
		return keyList;
	}

	/** 选择出现次数大于一定阈值的key list 
	 * 在产生模式时使用
	 * */
	public static List<String> getMostedKeysList(
			List<ProcessedUrl> processedUrlsList) {
		double length = processedUrlsList.size();
		HashMap<String, Integer> keyAndTimesMap = new HashMap<String, Integer>();
		for (ProcessedUrl url : processedUrlsList) {
			for (KeyValuePair pair : url.getKeyValuePairs()) {
				if (keyAndTimesMap.get(pair.getKeyStr()) == null) {
					keyAndTimesMap.put(pair.getKeyStr(), 1);
				} else {
					int times = keyAndTimesMap.get(pair.getKeyStr()) + 1;
					keyAndTimesMap.put(pair.getKeyStr(), times);
				}
			}
		}
		Iterator<String> iterator = keyAndTimesMap.keySet().iterator();
		List<String> list = new ArrayList<String>();
		while (iterator.hasNext()) {
			String keyStr = iterator.next();
			int times = keyAndTimesMap.get(keyStr);
			if (times / length > 0.9) {
				// System.out.println(keyStr+" "+times/length);
				list.add(keyStr);
			}
		}
		return list;
	}

	public static String getMostedKey(List<ProcessedUrl> processedUrlsList,
			List<String> noUseKeysList) {
		double length = processedUrlsList.size();
		HashMap<String, Integer> keyAndTimesMap = new HashMap<String, Integer>();
		for (ProcessedUrl url : processedUrlsList) {
			for (KeyValuePair pair : url.getKeyValuePairs()) {
				if (noUseKeysList.indexOf(pair.getKeyStr()) != -1) {
					continue;
				}
				if (keyAndTimesMap.get(pair.getKeyStr()) == null) {
					keyAndTimesMap.put(pair.getKeyStr(), 1);
				} else {
					int times = keyAndTimesMap.get(pair.getKeyStr()) + 1;
					keyAndTimesMap.put(pair.getKeyStr(), times);
				}
			}
		}
		int maxTimes = -1;
		String maxTimesKeyStr = null;
		Iterator<String> iterator = keyAndTimesMap.keySet().iterator();
		while (iterator.hasNext()) {
			String keyStr = iterator.next();
			int times = keyAndTimesMap.get(keyStr);
			// System.out.println("无common Key时："+keyStr+" "+times);
			if (times > maxTimes) {
				maxTimes = times;
				maxTimesKeyStr = keyStr;
			}
		}
		keyAndTimesMap = null;// ##
		if (maxTimes / length > 0.99) {
			return maxTimesKeyStr;
		} else {
			return null;
		}
	}
	public static String getMostedKeyForNoSplitKey(List<ProcessedUrl> processedUrlsList,
			List<String> noUseKeysList) {
		double length = processedUrlsList.size();
		HashMap<String, Integer> keyAndTimesMap = new HashMap<String, Integer>();
		for (ProcessedUrl url : processedUrlsList) {
			for (KeyValuePair pair : url.getKeyValuePairs()) {
				if (noUseKeysList.indexOf(pair.getKeyStr()) != -1) {
					continue;
				}
				if (keyAndTimesMap.get(pair.getKeyStr()) == null) {
					keyAndTimesMap.put(pair.getKeyStr(), 1);
				} else {
					int times = keyAndTimesMap.get(pair.getKeyStr()) + 1;
					keyAndTimesMap.put(pair.getKeyStr(), times);
				}
			}
		}
		int maxTimes = -1;
		String maxTimesKeyStr = null;
		Iterator<String> iterator = keyAndTimesMap.keySet().iterator();
		while (iterator.hasNext()) {
			String keyStr = iterator.next();
			int times = keyAndTimesMap.get(keyStr);
//			System.out.println(keyStr+" "+times);
			if (times > maxTimes) {
				maxTimes = times;
				maxTimesKeyStr = keyStr;
			}
		}
//		System.out.println(maxTimesKeyStr+" "+maxTimes+" "+length);
		if(maxTimes>100){
			return maxTimesKeyStr;
		}
		if (maxTimes / length > 0.5) {
			return maxTimesKeyStr;
		} else {
			return null;
		}
	}
	/**
	 * 根据flag的值判断keyStr前应该是/或者是?还是&，并拼接
	 * 在CalculateUtil.getSortedUrl中使用
	 * */
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

	/**
	 * 根据previousKeyStr和keyStr对flag进行赋值，不同的flag值在拼接URL时keyStr前面是/或者是？还是&
	 * 在CalculateUtil.getSortedUrl中使用
	 * */
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

	public static HashMap<String, String> map = new HashMap<String, String>();

	static void init() {
		FileReader fileReader;
		try {
			fileReader = new FileReader("E://patternAndRules.txt");
			BufferedReader reader = new BufferedReader(fileReader);
			String lineStr, part[];
			while ((lineStr = reader.readLine()) != null) {
				part = lineStr.split("\\$\\$");
				System.out.println(lineStr);
				map.put(part[0], part[1]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static boolean match(String patternStr, String urlStr)
			throws MalformedURLException {
		List<String> patternKeysList = new ArrayList<String>();// 模式串中的key
		List<String> patternValuesList = new ArrayList<String>();// 模式串中的value(和KeyList顺序一致)
		String s[] = StringUtils.split(patternStr, "/");
		for (int i = 0; i < s.length; i++) {
			if (s[i] == null || s[i].equals(""))
				continue;
			patternKeysList.add(s[i].split(":")[0]);
			patternValuesList.add(s[i].split(":")[1]);
		}
		ProcessedUrl processedUrl = new ProcessedUrl(urlStr);
		// 对匹配串中的key进行遍历处理
		for (KeyValuePair pair : processedUrl.getKeyValuePairs()) {
			String keyStr = pair.getKeyStr();// 匹配串中该pair中的key
			String valueStr = pair.getValueStr();// 匹配串中该pair中的value
			int index = patternKeysList.indexOf(keyStr);
			if (index != -1) {// 模式串中有该key
				String patternValueStr = patternValuesList.get(index);// 模式串中key对应的value值
				// 使用patternOutputStr时使用*
				if (!patternValueStr.equals(".*")
						&& !valueStr.equals(patternValueStr)) {
					return false;
				}
			}
			// 模式串中没有该key则不考虑
		}
		// 对在模式串中出现的key但不在匹配串中出现的key进行处理，该种情况下返回false
		for (int i = 0; i < s.length; i++) {
			String keyStr = patternKeysList.get(i);
			String valueStr = processedUrl.getValueOfTheKeyInOneUrl(keyStr);
			if (valueStr == null) {
				return false;
			}
		}
		return true;
	}

	public static int sum = 0;

	public static String rewriteFromFile(String url)
			throws MalformedURLException {
		if (map.keySet().size() == 0) {
			init();
		}
		ProcessedUrl processedUrl = new ProcessedUrl(url);
		Iterator<String> iterator = map.keySet().iterator();
		String patternStr = null;
		while (iterator.hasNext()) {
			String tempStr = iterator.next();
			// System.out.println(getSortedUrl(url));
			// if(Pattern.matches(tempStr,getSortedUrl(url))){
			if (match(tempStr, url)) {
				patternStr = map.get(tempStr);
				sum++;
				// System.out.println(url);
				// System.out.println(tempStr);
				// System.out.println("-----------");
				break;
			}
		}
		if (patternStr == null) {
			return url;
		}
		String patternArr[] = patternStr.split("/");
		List<String> patternKeyList = new ArrayList<String>();
		HashMap<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < patternArr.length; i++) {
			String tempArr[] = patternArr[i].split(":");
			patternKeyList.add(tempArr[0]);
			map.put(tempArr[0], tempArr[1]);
		}
		String resultStr = "http:/", keyStr, valueStr;
		String patternKeyArr[] = CalculateUtil.sortList(patternKeyList);
		for (int i = 0; i < patternArr.length; i++) {
			keyStr = patternKeyArr[i];
			if (keyStr == null || keyStr.length() == 0)
				continue;
			valueStr = map.get(keyStr);
			int flag;
			if (i > 0) {
				flag = checkFlag(patternKeyArr[i - 1], patternKeyArr[i]);
			} else {
				flag = checkFlag(null, patternKeyArr[i]);
			}
			if (valueStr.startsWith("#")) {// keep
			// resultStr += getStandardKeyValueStr(patternKeyArr[i], valueStr,
			// flag);
				resultStr += getStandardKeyValueStr(patternKeyArr[i],
						valueStr.substring(1), flag);
			} else if (valueStr.startsWith("*")) {// ignore
			// resultStr += getStandardKeyValueStr(patternKeyArr[i], valueStr,
			// flag);
				resultStr += getStandardKeyValueStr(patternKeyArr[i],
						valueStr.substring(1), flag);
			} else {// replace
				resultStr += getStandardKeyValueStr(patternKeyArr[i],
						processedUrl.getValueOfTheKeyInOneUrl(valueStr), flag);
			}
		}
		System.out.println(url);
		System.out.println(resultStr);
		System.out.println("---------------");
		// System.out.println("endNode pattern:"+endNode.getPatternOutputStr());
		return resultStr;
	}
}