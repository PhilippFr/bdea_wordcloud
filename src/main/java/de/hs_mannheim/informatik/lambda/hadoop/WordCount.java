package de.hs_mannheim.informatik.lambda.hadoop;

import com.kennycason.kumo.WordFrequency;
import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.nio.file.Path;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.log4j.BasicConfigurator;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//			String[] worte = value.toString().split("\\W+");
			//
			//			for (String wort : worte) {
			//				word.set(wort);
			//				context.write(word, one);
			//			}

			Pattern pattern = Pattern.compile("(\\b[^\\s]+\\b)");
			Matcher matcher = pattern.matcher(value.toString());
			while (matcher.find()) {
				word.set(value.toString().substring(matcher.start(), matcher.end()).toLowerCase());
				context.write(word, one);
			}
		}
	}

	public static class SwitchMapper extends Mapper<Text, IntWritable, IntWritable, Text>{

		public void map(Text word, IntWritable count, Context context) throws IOException, InterruptedException {
			context.write(count, word);
		}

	}

	public static List<WordFrequency> tagCloudWordFrequency(Path path) throws IOException, ClassNotFoundException, InterruptedException {
		BasicConfigurator.configure(); 					// Log4j Config oder ConfigFile in Resources Folder
		System.setProperty("hadoop.home.dir", System.getenv("HADOOP_HOME"));

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(4);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(path.toAbsolutePath().toString()));
		FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("/tmp/wc-output"));

		job.waitForCompletion(true);

		// -----------------------> Job 2

		job = Job.getInstance(conf, "freq sort");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(SwitchMapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(4);

		job.setSortComparatorClass(MyDescendingComparator.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path("/tmp/wc-output"));

		org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path("/tmp/fs-output");
		FileOutputFormat.setOutputPath(job, outputPath);

		File directory = new File(outputPath.toUri());

		System.out.println(directory);
		System.exit(1);

		List<WordFrequency> wordFrequencies = new ArrayList();

		/*
		Map<String, Integer> cloud =
		cloud.forEach((key, value) -> {
			wordFrequencies.add(new WordFrequency(key, value));
		});

		 */
		return wordFrequencies;
	}

	/*
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure(); 					// Log4j Config oder ConfigFile in Resources Folder
		System.setProperty("hadoop.home.dir", "/");  	// für Hadoop 3.3.0

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(4);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("resources/klassiker/*.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/wc-output"));

		job.waitForCompletion(true);

		// -----------------------> Job 2

		job = Job.getInstance(conf, "freq sort");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(SwitchMapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(4);

		job.setSortComparatorClass(MyDescendingComparator.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.addInputPath(job, new Path("/tmp/wc-output"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/fs-output"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	 */

}

class MyDescendingComparator extends WritableComparator {
	public MyDescendingComparator() {
		super(IntWritable.class, true);
	}

	public int compare(WritableComparable a, WritableComparable b) {
		return super.compare(a, b) * (-1);
	}
}