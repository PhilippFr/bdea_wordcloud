package de.hs_mannheim.informatik.lambda.hadoop;

import com.kennycason.kumo.WordFrequency;
import org.apache.hadoop.conf.Configuration;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Pattern pattern = Pattern.compile("(\\b[^\\s]+\\b)");
            Matcher matcher = pattern.matcher(value.toString());
            while (matcher.find()) {
                word.set(value.toString().substring(matcher.start(), matcher.end()).toLowerCase());
                context.write(word, one);
            }
        }
    }

    public static class SwitchMapper extends Mapper<Text, IntWritable, IntWritable, Text> {

        public void map(Text word, IntWritable count, Context context) throws IOException, InterruptedException {
            context.write(count, word);
        }

    }

    public static void deleteDirectoryStream(Path path) throws IOException {
        Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    public static List<WordFrequency> tagCloudWordFrequency(Path path) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();                    // Log4j Config oder ConfigFile in Resources Folder
        System.setProperty("hadoop.home.dir", "/");

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
        deleteDirectoryStream(new File("/tmp/wc-output").toPath());
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

        deleteDirectoryStream(new File("/tmp/fs-output").toPath());
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path("/tmp/fs-output");
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

        List<WordFrequency> wordFrequencies = new ArrayList();

        File[] files = new File("/tmp/fs-output").listFiles();
        for (int i = 0; i < files.length; i++) {
            String name = files[i].getName();
            if (files[i].getName().startsWith("part-r-")) {
                FileInputStream fileInputStream = null;
                byte[] bFile = new byte[(int) files[i].length()];

                //convert file into array of bytes
                fileInputStream = new FileInputStream(files[i]);
                fileInputStream.read(bFile);
                fileInputStream.close();

                String[] bString = new String(bFile, StandardCharsets.UTF_8).split("\n");
                String[] keyValue;
                for (String row : bString) {
                    keyValue = row.split("\t");
                    if (keyValue[1].length() >= 4) {
                        wordFrequencies.add(new WordFrequency(keyValue[1], Integer.parseInt(keyValue[0])));
                    }
                }
            }
        }
        Collections.sort(wordFrequencies);
        wordFrequencies = wordFrequencies.subList(0, 300);

        return wordFrequencies;
    }
}

class MyDescendingComparator extends WritableComparator {
    public MyDescendingComparator() {
        super(IntWritable.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        return super.compare(a, b) * (-1);
    }
}