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
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.BytesWritable;
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
import com.iflytek.gas.process.ParseAndMergeJob.MultipleOutputsConst;
import com.iflytek.parse.config.OdisLibConfig;
import com.iflytek.parse.html.commons.PageType;

public class GarbageStatisticJob {
	private final static String NAME = "TestJob";
	private static final Logger log = LoggerFactory
			.getLogger(GarbageStatisticJob.class);
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
		conf.set("mapreduce.reduce.memory.mb", "12288");// reduce的占内存设置
		conf.set("mapreduce.reduce.java.opts", "-Xmx10240m");
		conf.set("yarn.nodemanager.pmem-check-enabled", "false");
		conf.set("yarn.nodemanager.vmem-check-enabled", "false");

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
			job.setJarByClass(GarbageStatisticJob.class);
			job.setJobName(NAME + "_" + inpath + "_" + outpath);

			HDFSFileUtils.removeFile(outpath);
			// output.getFileSystem(job).delete(output, true);

			job.setMapperClass(UrlStateMergeMap.class);
			job.setReducerClass(UrlStateMergeReduce.class);

			// 输入输出文件格式
			// job.setOutputFormatClass(AvroKeyOutputFormat.class);
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
				String host = new URL(url).getHost();
				context.write(new AvroKey<String>(garbagePageTypeStr+"##"+host),
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

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			BaseConfig.getInstance().loadConfFromJobConf(
					context.getConfiguration());
			super.setup(context);
		}
		@Override
		protected void reduce(AvroKey<String> _key,
				Iterable<AvroValue<String>> values, Context context)
				throws IOException, InterruptedException {
			String typeAndHostStr = new Utf8(_key.toString()).toString();
			long start = System.nanoTime();
			try {
				List<ProcessedUrl> urlsList = new ArrayList<ProcessedUrl>();
				int sum=0;
				for (AvroValue<String> item : values) {
					sum++;
				}
				if(sum>100000){
					context.write(new AvroKey<String>(typeAndHostStr),new AvroValue<String>(""+sum));
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
	}
}
