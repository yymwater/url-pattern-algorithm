package com.flytek.urlpattern.tree.applications;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flytek.urlpattern.tree.CalculateUtil;
import com.flytek.urlpattern.tree.Fun;
import com.flytek.urlpattern.tree.ProcessedUrl;
import com.flytek.urlpattern.tree.TreeNode;
import com.iflytek.common.HDFSFileUtils;
import com.iflytek.fs.avro.generate.DownloadPage;
import com.iflytek.fs.avro.generate.IndexPage;
import com.iflytek.fs.avro.generate.RawPage;
import com.iflytek.gas.config.BaseConfig;
import com.iflytek.gas.config.MRConfiguration;
import com.iflytek.gas.counters.FSCounters;
import com.iflytek.gas.counters.TimeCounters;
import com.iflytek.parse.config.OdisLibConfig;
import com.iflytek.parse.html.commons.PageType;

public class GarbagePageUrlPatternJob {
	private final static String NAME = "GarbagePageUrlPatternJob";
	private static final Logger log = LoggerFactory
			.getLogger(GarbagePageUrlPatternJob.class);
	public static void main(String[] args) throws Exception {
//		Configuration conf = MRConfiguration.createUpdateQueqeConf();
		Configuration conf =  HBaseConfiguration.create();
		conf.set("mapred.job.queue.name","offline");
//		new TestJob().fun(conf);		// 设置小点,OOM
		conf.setFloat("mapred.job.shuffle.input.buffer.percent", 0.40f);
		// conf.set("mapred.child.java.opts", "-Xmx64768m");
		// 完成85% 才启动reduce
		conf.setFloat("mapred.reduce.slowstart.completed.maps", 0.99f);// map运行多少时，reduce启动
		conf.setInt("mapred.task.timeout", 0);// 不设置超时
		conf.set("mapreduce.reduce.memory.mb", "6144");// reduce的占内存设置
		conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("ERROR: Wrong number of parameters: "
					+ args.length);
			System.err.println("Usage: " + NAME
					+ " <inputpath> <outputpath> <reduce-num>");
			System.exit(-1);
		}
		int reduceNum = Integer.parseInt(otherArgs[2]);
		log.info(NAME + " : starting");
		Job job = CreateParseJob(conf, otherArgs[0], otherArgs[1], reduceNum);
		boolean success = job.waitForCompletion(true);
		if (success) {
			log.info(NAME + " : done.");
		} else {
			log.info(NAME + " : failed");
		}
	}

	public static Job CreateParseJob(Configuration conf, String inpath,
			String outpath, int reduceNum) {
		try {
			@SuppressWarnings("deprecation")
			Job job = new Job(conf);
			job.setJarByClass(GarbagePageUrlPatternJob.class);
			job.setJobName(NAME + "_" + inpath + "_" + outpath);

			HDFSFileUtils.removeFile(outpath);
			// output.getFileSystem(job).delete(output, true);

			job.setMapperClass(UrlStateMergeMap.class);
			job.setReducerClass(UrlStateMergeReduce.class);

			// 输入输出文件格式
//			 job.setOutputFormatClass(AvroKeyOutputFormat.class);
			job.setInputFormatClass(AvroKeyValueInputFormat.class);
			job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
			
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(Text.class);
			String[] pathsStrings = inpath.split("\\+");
			for (String path : pathsStrings) {
				log.info("add input path " + path);
				AvroKeyValueInputFormat.addInputPath(job, new Path(path));
			}

			FileOutputFormat.setOutputPath(job, new Path(outpath));

			// 设置Schema
			// AvroJob.setInputSchema(job, IndexPage.SCHEMA$);
			AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
			AvroJob.setInputValueSchema(job, RawPage.SCHEMA$);
			AvroJob.setMapOutputKeySchema(job,
					Schema.create(Schema.Type.STRING));
			AvroJob.setMapOutputValueSchema(job,Schema.create(Schema.Type.STRING));

			AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
			AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.STRING));

			job.setNumReduceTasks(reduceNum);

			return job;
		} catch (Exception e) {
			e.printStackTrace(); 
			return null;
		}
	}

	/**
	 * 
	 * @author "hpma@iflytek.com"
	 * 
	 */
	static class UrlStateMergeMap
			extends
			Mapper<AvroKey<String>, AvroValue<RawPage>, AvroKey<String>, AvroValue<String>> {
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}
		@Override
		protected void map(AvroKey<String> key, AvroValue<RawPage> value,
				Context context) throws IOException, InterruptedException {
			context.getCounter(FSCounters.Rows).increment(1);
			String garbagePageTypeStr = new Utf8(key.toString()).toString();
			try {
				// 按domain来排序
				RawPage page = value.datum();
				String url = page.getUrl().toString();
				String host =new URL(url).getHost();
				if(host.equals("m.ku6.com")){
					Random random=new Random();
					if(random.nextDouble()<0.33){
						context.write(new AvroKey<String>(host),
								new AvroValue<String>(url));
					}
					return;
				}
				if(host.equals("m.newsmth.net")){
					Random random=new Random();
					if(random.nextDouble()<0.5){
						context.write(new AvroKey<String>(host),
								new AvroValue<String>(url));
					}
					return;
				}
				context.write(new AvroKey<String>(host),
						new AvroValue<String>(url));
			} catch (Exception e) {
				context.getCounter(FSCounters.Error).increment(1);
				e.printStackTrace();
			}
		}
	}

	/**
	 * 
	 * 
	 * @author "hpma@iflytek.com"
	 * 
	 */
	static class UrlStateMergeReduce
			extends
			Reducer<AvroKey<String>, AvroValue<String>,AvroKey<String>,AvroValue<String>> {
		private String fileNameStr = "/work/ymyang2/garbage_prepare";
		private final HashMap <String,String> map=new HashMap<String,String>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			//从HDFS文件中进行加载pattern
			Configuration config = new Configuration();
			FileSystem hdfs = null;
			try {
				hdfs = FileSystem.get(config);
				Path inputDir=new Path(fileNameStr);
				FileStatus []inputFiles=hdfs.listStatus(inputDir);
				if(inputFiles!=null){
					for (int i = 1; i < inputFiles.length; i++) {
						Path destFile=inputFiles[i].getPath();					
						try {
							InputStream is = hdfs.open(destFile);
							DataFileStream<GenericRecord> reader = new DataFileStream<GenericRecord>(
									is, new SpecificDatumReader<GenericRecord>());
							while (reader.hasNext()) {
								GenericRecord item = reader.next();
								AvroKeyValue<Utf8,Utf8> data = new AvroKeyValue<Utf8, Utf8>(
										item);
								map.put(data.getKey().toString(),data.getValue().toString());
							}
							IOUtils.cleanup(null, is);
							IOUtils.cleanup(null, reader);
						}catch(IOException ex) {
							ex.printStackTrace();
						}
					}
				}
				//在离线阶段有10个reducer在运行，则有10个结果文件
			} catch (IOException e) {
				e.printStackTrace();
			}
			BaseConfig.getInstance().loadConfFromJobConf(
					context.getConfiguration());
			super.setup(context);
		}
		@Override
		protected void reduce(AvroKey<String> _key,
				Iterable<AvroValue<String>> values, Context context)
				throws IOException, InterruptedException {
			int themePageCount = 0;
			String host = new Utf8(_key.toString()).toString();
			
			int standardsum=500000;
			if(host.equals("m.newsmth.net")){
				standardsum=1000000;
			}else if(host.equals("m.ku6.com")){
				standardsum=1500000;				
			}
			
			long start = System.nanoTime();
			try {
				List<ProcessedUrl> urlsList = new ArrayList<ProcessedUrl>();
				int sum=0;
				if(map.get(host)!=null){
					sum=Integer.parseInt(map.get(host));
				}
				Random random=new Random();
				// 放入列表
				for (AvroValue<String> item : values) {
					themePageCount++;
					if(sum>standardsum&&random.nextDouble()>standardsum*1.0/sum)
						continue;
					String urlStr = item.datum();
					urlsList.add(new ProcessedUrl(urlStr));
				}
//				context.write(new AvroKey<String>(host),new AvroValue<String>("##"+themePageCount));
				if (urlsList.size() <50) {// 如果该domain没有themePage则退出
					return;
				}
				/**抽取测试样例*/
				/**int testSum=0;
				for(ProcessedUrl url:urlsList){
					if(random.nextDouble()<0.0001&&testSum<30){
						testSum++;
						context.write(new AvroKey<String>("##"),new AvroValue<String>(url.getUrlStr()));
					}
				}*/
				List<String> keyDoneList = new ArrayList<String>();
				List<ProcessedUrl>trainList=new ArrayList<ProcessedUrl>();
				List<ProcessedUrl>testList=new ArrayList<ProcessedUrl>();
				for(int i=0;i<urlsList.size()*0.75;i++){
					trainList.add(urlsList.get(i));
				}
				for(int i=(int) (urlsList.size()*0.75)+1;i<urlsList.size();i++){
					testList.add(urlsList.get(i));
				}
				Fun fun=new Fun();
				TreeNode rootNodeOfPatternTree = fun.buildPatternTree(trainList,
						keyDoneList, null, null);
				String resultStr=getLeafTreeNodesPatterns(rootNodeOfPatternTree);
				/**起过滤某些匹配度不高的URL Pattern*/
				Pattern pattern=Pattern.compile(resultStr);
				if(urlsList.size()<5000){
					int matchSum=0;
					for(ProcessedUrl url:trainList){
						if(pattern.matcher(CalculateUtil.getSortedUrl(url.getUrlStr())).find()){
							matchSum++;
						}
					}
					if(matchSum*1.0/trainList.size()<0.5){
						return;
					}
				}else{
					int matchSum=0,allSum=0;
					for(ProcessedUrl url:trainList){
						if(random.nextDouble()>5000.0/trainList.size())
							continue;
						allSum++;
						if(pattern.matcher(CalculateUtil.getSortedUrl(url.getUrlStr())).find()){
							matchSum++;
						}
					}
					if(matchSum*1.0/allSum<0.5){
						return;
					}
				}
				int matchSum=0;
				for(int i=0;i<testList.size();i++){
					if(pattern.matcher(CalculateUtil.getSortedUrl(testList.get(i).getUrlStr())).find()){
						matchSum++;
					}
				}
				context.write(new AvroKey<String>(host),new AvroValue<String>(resultStr+"$$"+themePageCount));
				context.write(new AvroKey<String>("#"+matchSum),new AvroValue<String>(""+testList.size()));
				context.getCounter(TimeCounters.UrlPattern)
						.increment(
								TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
										- start));
			} catch (Exception e) {
				context.getCounter(FSCounters.Error).increment(1);
				e.printStackTrace();
			}
		}

		public static String getLeafTreeNodesPatterns(TreeNode rootNode){
			String patternsStr="";
			Queue<TreeNode>queue=new LinkedList<TreeNode>();
			queue.add(rootNode);
			TreeNode currentNode;
			boolean isFirst=true;
			while(!queue.isEmpty()){
				currentNode=queue.poll();
				if(currentNode.getChildNodes()!=null&&currentNode.getChildNodes().size()>0){
					boolean allChildIsLeafFlag=true;
					for(TreeNode treeNode:currentNode.getChildNodes()){
						if(treeNode.getChildNodes()!=null&&treeNode.getChildNodes().size()>0){
							allChildIsLeafFlag=false;
							break;
						}
					}
					if(currentNode.isChildHasStarFlag()&&allChildIsLeafFlag){
						queue.add(currentNode.getChildNodes().get(0));
						continue;
					}
					for(TreeNode node:currentNode.getChildNodes()){
						queue.add(node);
					}
				}else{
					String patternStr="";
					for(char c:currentNode.getPatternOutputStr().toCharArray()){
						if(c=='*'){
							patternStr+=".*";
						}else{
							patternStr+=c;
						}
					}
					if(isFirst){
						patternsStr+=patternStr;
						isFirst=false;
					}else{
						patternsStr+="|"+patternStr;
					}
				}
			}
			return patternsStr;
		}
	}
}

