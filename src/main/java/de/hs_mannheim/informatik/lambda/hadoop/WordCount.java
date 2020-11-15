package de.hs_mannheim.informatik.lambda.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// String[] spalten = value.toString().split(",");
			Pattern pattern = Pattern.compile("(\\b[^\\s]+\\b)");
			Matcher matcher = pattern.matcher(value.toString());
			while (matcher.find()) {
				Text word = new Text();
				word.set(value.toString().substring(matcher.start(), matcher.end()).toLowerCase());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class WordcountComparator extends WritableComparator {
		public WordcountComparator(){
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b){
			return super.compare(a, b) * -1;
		}
	}

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure(); 					// Log4j Config oder ConfigFile in Resources Folder
		System.setProperty("hadoop.home.dir", System.getenv("HADOOP_HOME"));

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "card count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setSortComparatorClass(WordcountComparator.class);
		
		FileInputFormat.addInputPath(job, new Path("resources/*.txt"));
		FileOutputFormat.setOutputPath(job, new Path("resources/wordCount-output" + System.currentTimeMillis()));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}