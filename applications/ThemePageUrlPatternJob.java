package com.flytek.urlpattern.tree.applications;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flytek.urlpattern.tree.Fun;
import com.flytek.urlpattern.tree.ProcessedUrl;
import com.flytek.urlpattern.tree.TreeNode;

import com.iflytek.common.HDFSFileUtils;
import com.iflytek.fs.avro.generate.DownloadPage;
import com.iflytek.fs.avro.generate.IndexPage;
import com.iflytek.gas.config.BaseConfig;
import com.iflytek.gas.config.MRConfiguration;
import com.iflytek.gas.counters.FSCounters;
import com.iflytek.gas.counters.TimeCounters;


public class ThemePageUrlPatternJob {
	private final static String NAME = "ThemePageUrlPatternJob";
	private static final Logger log = LoggerFactory
			.getLogger(ThemePageUrlPatternJob.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = MRConfiguration.createUpdateQueqeConf();
		// 设置小点,OOM
		conf.setFloat("mapred.job.shuffle.input.buffer.percent", 0.40f);
		// conf.set("mapred.child.java.opts", "-Xmx64768m");
		// 完成85% 才启动reduce
		conf.setFloat("mapred.reduce.slowstart.completed.maps", 0.99f);// map运行多少时，reduce启动
		conf.setInt("mapred.task.timeout", 0);// 不设置超时
		conf.set("mapreduce.reduce.memory.mb", "12288");// reduce的占内存设置
		conf.set("mapreduce.reduce.java.opts", "-Xmx10240m");

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
			Job job = new Job(conf);
			job.setJarByClass(ThemePageUrlPatternJob.class);
			job.setJobName(NAME + "_" + inpath + "_" + outpath);

			HDFSFileUtils.removeFile(outpath);
			// output.getFileSystem(job).delete(output, true);

			job.setMapperClass(UrlStateMergeMap.class);
			job.setReducerClass(UrlStateMergeReduce.class);

			// 输入输出文件格式
			// job.setOutputFormatClass(AvroKeyOutputFormat.class);
			job.setInputFormatClass(AvroKeyValueInputFormat.class);
			job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

			String[] pathsStrings = inpath.split("\\+");
			for (String path : pathsStrings) {
				log.info("add input path " + path);
				AvroKeyValueInputFormat.addInputPath(job, new Path(path));
			}

			AvroKeyValueOutputFormat.setOutputPath(job, new Path(outpath));

			// 设置Schema
			// AvroJob.setInputSchema(job, IndexPage.SCHEMA$);
			AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
			AvroJob.setInputValueSchema(job, DownloadPage.SCHEMA$);
			AvroJob.setMapOutputKeySchema(job,
					Schema.create(Schema.Type.STRING));
			AvroJob.setMapOutputValueSchema(job, DownloadPage.SCHEMA$);

			AvroJob.setOutputKeySchema(job, DownloadPage.SCHEMA$);
			AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.STRING));
			// job.setOutputKeyClass(Text.class);
			// job.setOutputValueClass(BytesWritable.class);

			AvroMultipleOutputs.addNamedOutput(job, "HostThemePagePattern",
					AvroKeyValueOutputFormat.class,
					Schema.create(Schema.Type.STRING),
					Schema.create(Schema.Type.STRING));

			AvroMultipleOutputs.addNamedOutput(job, "ThemePageUrl",
					AvroKeyOutputFormat.class,
					Schema.create(Schema.Type.STRING));
			//
			// AvroMultipleOutputs.addNamedOutput(job, "HPRDppi",
			// AvroKeyOutputFormat.class,
			// Schema.create(Schema.Type.STRING));

			// AvroMultipleOutputs.addNamedOutput(job, "urlpattern",
			// AvroKeyOutputFormat.class,
			// Schema.create(Schema.Type.STRING));

			// amos.write("urlpattern", rule.toString());
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
			Mapper<AvroKey<String>, AvroValue<DownloadPage>, AvroKey<String>, AvroValue<DownloadPage>> {

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			super.setup(context);
		}

		@Override
		protected void map(AvroKey<String> key, AvroValue<DownloadPage> value,
				Context context) throws IOException, InterruptedException {
			context.getCounter(FSCounters.Rows).increment(1);
			try {
				// 按domain来排序
				DownloadPage page = value.datum();
				String host = page.getIndexPage().getHost().toString();
				context.write(new AvroKey<String>(host),
						new AvroValue<DownloadPage>(page));

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
			Reducer<AvroKey<String>, AvroValue<DownloadPage>, AvroKey<DownloadPage>, AvroValue<String>> {
		private AvroMultipleOutputs amos;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.amos = new AvroMultipleOutputs(context);
			BaseConfig.getInstance().loadConfFromJobConf(
					context.getConfiguration());
			// getGoodSeeds();
			super.setup(context);
		}

		protected void reduce(AvroKey<String> _key,
				Iterable<AvroValue<DownloadPage>> values, Context context)
				throws IOException, InterruptedException {
			int themePageCount = 0;
			String host = new Utf8(_key.toString()).toString();
			long start = System.nanoTime();
			try {
				List<ProcessedUrl> urlsList = new ArrayList<ProcessedUrl>();
				// 放入列表
				List<DownloadPage> pagesList = new ArrayList<DownloadPage>();
				for (AvroValue<DownloadPage> item : values) {
					DownloadPage page = item.datum();
					pagesList.add(page);
					IndexPage ipage = page.getIndexPage();
					if (ipage.getType().toString().equals("THEME_PAGE")) {
						themePageCount++;
						String url = page.getUrl().toString();
						ProcessedUrl processedUrl = new ProcessedUrl(url);
						urlsList.add(processedUrl);
					}
				}
				if (urlsList.size() == 0) {// 如果该domain没有themePage则退出
					return;
				}
				log.info("the host is\t" + host + "\t"
						+ "the theme_page counts is\t" + themePageCount);
				List<String> keyDoneList = new ArrayList<String>();
				Fun fun=new Fun();
				TreeNode rootNodeOfPatternTree = fun.buildPatternTree(urlsList,
						keyDoneList, null, null);
				List<TreeNode> leafNodesList = new ArrayList<TreeNode>();
				getLeaf(rootNodeOfPatternTree, leafNodesList);
				// log.info("#### leaf size:"+leafNodesList.size());
				// log.info("#### pages size:"+pagesList.size());
				for (DownloadPage page : pagesList) {
					boolean isThemePage = analyzePageType(page, leafNodesList);
					context.write(new AvroKey<DownloadPage>(page),
							new AvroValue<String>(""));
					if (isThemePage) {
						// log.info(page.getIndexPage().getUrl().toString());
						amos.write("ThemePageUrl", new AvroKey<String>(page
								.getUrl().toString()));
						// context.write(new AvroKey<DownloadPage>(page),new
						// AvroValue<String>(""));
					}
				}
				if (leafNodesList.size() > 0) {
					for (TreeNode leaf : leafNodesList) {
						amos.write(
								"HostThemePagePattern",
								new AvroKey<String>(host),
								new AvroValue<String>(leaf
										.getPatternOutputStr()));
					}
				}
				context.getCounter(TimeCounters.UrlPattern)
						.increment(
								TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
										- start));
			} catch (Exception e) {
				context.getCounter(FSCounters.Error).increment(1);
				e.printStackTrace();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			close();
			super.cleanup(context);
		}

		private boolean analyzePageType(DownloadPage page, List<TreeNode> leafs) {
			String url = page.getUrl().toString();
			boolean flag = false;
			for (TreeNode leaf : leafs) {// 遍历叶子节点
				// log.info("###"+leaf.getPatternOutputStr()+" "+url);
				if (leaf.match(url)) {
					flag = true;
					break;
				} else {
					// log.info("#"+leaf.getPatternOutputStr());
					// log.info(url);
				}
			}
			if (flag) {
				page.getIndexPage().setType("THEME_PAGE");
				return true;
			} else {
				return false;
			}
		}

		private void getLeaf(TreeNode tn, List<TreeNode> leafs) {
			if (tn.getChildNodes() == null || tn.getChildNodes().size() == 0) {
				leafs.add(tn);
			} else {
				List<TreeNode> childNodes = tn.getChildNodes();
				for (TreeNode tn1 : childNodes) {
					getLeaf(tn1, leafs);
				}
			}
		}

		protected void close() throws IOException {
			try {
				amos.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
