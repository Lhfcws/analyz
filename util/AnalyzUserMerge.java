package com.yeezhao.analyz.util;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;

public class AnalyzUserMerge implements CliRunner {
	
	static IntWritable one = new IntWritable(1);

	public static class MergeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable offset, Text line, Context context) {
			try {
				context.write(line, one);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class MergeReducer extends Reducer<Text, IntWritable, Text, Text> {
		
		public Text opText = new Text(AppConsts.ALL_ANALYZ_OP);
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) {
			String[] segs = key.toString().split("\\s+");
			try {
				context.write(new Text(segs[0]), opText);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		AdvCli.initRunner(args, "userMerge", new AnalyzUserMerge());
	}

	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options opts = new Options();
		opts.addOption(AdvCli.CLI_PARAM_HELP, false, "print help message");
		opts.addOption(AdvCli.CLI_PARAM_I, true, "input hdfs file");
		opts.addOption(OptionBuilder.withArgName("output").hasArg()
				.withDescription("output").create(AdvCli.CLI_PARAM_O));
		return opts;
	}

	@Override
	public void start(CommandLine cmdLine) {
		try {
			
		Configuration conf = AnalyzConfiguration.getInstance();
		String operationsFile = cmdLine.getOptionValue(AdvCli.CLI_PARAM_I);
		Job job = new Job(conf, "user merge, input file=" + operationsFile);
		String outputDir = cmdLine.getOptionValue(AdvCli.CLI_PARAM_O);
		job.setJarByClass(AnalyzUserMerge.class);
		FileInputFormat.addInputPath(job, new Path(operationsFile));
		job.setMapperClass(MergeMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(MergeReducer.class);
		FileOutputFormat.setOutputPath(job, new Path( outputDir));
		job.waitForCompletion(true) ;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		return cmdLine.hasOption(AdvCli.CLI_PARAM_I);
	}

}
