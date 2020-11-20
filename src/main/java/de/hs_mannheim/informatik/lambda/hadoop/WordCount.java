package de.hs_mannheim.informatik.lambda.hadoop;

import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.CircleBackground;
import com.kennycason.kumo.font.scale.SqrtFontScalar;
import com.kennycason.kumo.palette.ColorPalette;
import de.hs_mannheim.informatik.lambda.controller.LambdaController;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.log4j.BasicConfigurator;

import java.awt.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.io.FilenameUtils.removeExtension;

public class WordCount {

    public static void deleteDirectoryStream(Path path) throws IOException {
        File file = new File(path.toString());
        if (file.isDirectory()) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    public static void normalizedWordFrequency(Path path) throws IOException, ClassNotFoundException, InterruptedException {
        long now = System.currentTimeMillis();

        BasicConfigurator.configure();                    // Log4j Config oder ConfigFile in Resources Folder
        System.setProperty("hadoop.home.dir", "/");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "wordFrequency");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(path.toAbsolutePath().toString()));

        /*
        // deleteDirectoryStream(new File("/tmp/wc-output").toPath());
        org.apache.hadoop.fs.Path wcPath = new org.apache.hadoop.fs.Path("/tmp/wc-output-" + now);
        SequenceFileOutputFormat.setOutputPath(job, wcPath);
        */

        deleteDirectoryStream(new File(LambdaController.TERM_FREQUENCIES_PATH + removeExtension(path.getFileName().toString())).toPath());
        org.apache.hadoop.fs.Path wcOutputPath = new org.apache.hadoop.fs.Path(LambdaController.TERM_FREQUENCIES_PATH + removeExtension(path.getFileName().toString()));
        FileOutputFormat.setOutputPath(job, wcOutputPath);

        job.waitForCompletion(true);


        /*
        // -----------------------> Job 2

        job = Job.getInstance(conf, "frequencySort");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(IntSwitchMapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);

        job.setSortComparatorClass(MyDescendingIntComparator.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);


        SequenceFileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path("/tmp/wc-output-" + now));

        deleteDirectoryStream(new File(LambdaController.TERM_FREQUENCIES_PATH + removeExtension(path.getFileName().toString())).toPath());
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(LambdaController.TERM_FREQUENCIES_PATH + removeExtension(path.getFileName().toString()));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);


        // -----------------------> Determine highest frequency


        ArrayList<String> topRows = new ArrayList<>();

        File[] files = new File("/tmp/fs-output-" + now).listFiles();
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
                topRows.add(bString[0]);
            }
        }

        String[] keyValue;
        String mostFrequentWord = "";
        int mostFrequentWordFrequency = 0;

        for (String row : topRows) {
            keyValue = row.split("\t");
            if (Integer.parseInt(keyValue[0]) > mostFrequentWordFrequency) {
                mostFrequentWord = keyValue[1];
                mostFrequentWordFrequency = Integer.parseInt(keyValue[0]);
            }
        }

        System.out.println("mostFrequentWord: " + mostFrequentWord + " " + mostFrequentWordFrequency);

        conf.setInt("mostFrequentWordFrequency", mostFrequentWordFrequency);

        // -----------------------> Job 3

        job = Job.getInstance(conf, "normalizeWordFrequency");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);

        // job.setSortComparatorClass(MyDescendingDoubleComparator.class);

        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path("/tmp/fs-output-" + now));

        deleteDirectoryStream(new File(LambdaController.NORMALIZED_TERM_FREQUENCIES_PATH + removeExtension(path.getFileName().toString())).toPath());
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(LambdaController.NORMALIZED_TERM_FREQUENCIES_PATH + removeExtension(path.getFileName().toString()));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

        */

        // -----------------------> Hadoop finished

        documentFrequency();
        singleTfIdf(wcOutputPath);
    }

    public static void documentFrequency() throws IOException, ClassNotFoundException, InterruptedException {
        long now = System.currentTimeMillis();

        BasicConfigurator.configure();                    // Log4j Config oder ConfigFile in Resources Folder
        System.setProperty("hadoop.home.dir", "/");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "documentFrequency");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(DocumentFrequencyMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        File[] folders = new File(LambdaController.TERM_FREQUENCIES_PATH).listFiles();

        conf.setInt("numberOfDocuments", folders.length);

        org.apache.hadoop.fs.Path folderPath;

        for (File folder : folders) {
            if (folder.isDirectory()) {
                folderPath = new org.apache.hadoop.fs.Path(folder.getAbsolutePath());
                FileInputFormat.addInputPath(job, folderPath);
                System.out.println("Adding folder: " + folder.getName() + " at: " + folderPath);
            } else {
                System.out.println("FOUND NON FOLDER FILE: " + folder.getName());
            }
        }

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // deleteDirectoryStream(new File("/tmp/wc-output").toPath());
        SequenceFileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("/tmp/df-output-" + now));

        job.waitForCompletion(true);

        // -----------------------> Job 2

        job = Job.getInstance(conf, "inverseDocumentFrequency");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(InverseDocumentFrequencyMapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path("/tmp/df-output-" + now));

        deleteDirectoryStream(new File(LambdaController.INVERSE_DOCUMENT_FREQUENCIES_PATH).toPath());
        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(LambdaController.INVERSE_DOCUMENT_FREQUENCIES_PATH));

        job.waitForCompletion(true);


    }

    public static void singleTfIdf(org.apache.hadoop.fs.Path documentWcPath) throws IOException, ClassNotFoundException, InterruptedException {
        long now = System.currentTimeMillis();


        /*
        BasicConfigurator.configure();                    // Log4j Config oder ConfigFile in Resources Folder
        System.setProperty("hadoop.home.dir", "/");

        Configuration conf = new Configuration();


        // WC SequenceFileInput and Switch and file output

        Job job = Job.getInstance(conf, "frequencySwitcher");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        SequenceFileInputFormat.addInputPath(job, wcPath);

        // deleteDirectoryStream(new File("/tmp/fs-output-" + now).toPath());
        org.apache.hadoop.fs.Path wcFilePath = new org.apache.hadoop.fs.Path("/tmp/wc-file-output-" + now);
        FileOutputFormat.setOutputPath(job, wcFilePath);

        job.waitForCompletion(true);

         */

        // tfIdf Job

        BasicConfigurator.configure();                    // Log4j Config oder ConfigFile in Resources Folder
        System.setProperty("hadoop.home.dir", "/");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "wordFrequency");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TfIdfMapper.class);
        job.setReducerClass(TfIdfReducer.class);
        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // FileInputFormat.addInputPath(job, path);
        FileInputFormat.addInputPath(job, documentWcPath);
        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(LambdaController.INVERSE_DOCUMENT_FREQUENCIES_PATH));


        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("/tmp/tfIdf-output-" + now));

        job.waitForCompletion(true);

        // Job 2 - switch and sort

        job = Job.getInstance(conf, "switchAndSort");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(DoubleSwitchMapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);

        job.setSortComparatorClass(MyDescendingDoubleComparator.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path("/tmp/tfIdf-output-" + now));

        deleteDirectoryStream(new File(LambdaController.TD_IDF_PATH + removeExtension(documentWcPath.getName())).toPath());
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(LambdaController.TD_IDF_PATH + removeExtension(documentWcPath.getName()));

        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

        List<WordFrequency> wordFrequencies = new ArrayList<WordFrequency>();

        File[] outputFiles = new File(outputPath.toString()).listFiles();
        for (int i = 0; i < outputFiles.length; i++) {
            String name = outputFiles[i].getName();
            if (outputFiles[i].getName().startsWith("part-r-")) {
                FileInputStream fileInputStream = null;
                byte[] bFile = new byte[(int) outputFiles[i].length()];

                //convert file into array of bytes
                fileInputStream = new FileInputStream(outputFiles[i]);
                fileInputStream.read(bFile);
                fileInputStream.close();
                String[] bString = new String(bFile, StandardCharsets.UTF_8).split("\n");
                String[] keyValue;
                for (String row : bString) {
                    keyValue = row.split("\t");
                    int count = (int) Double.parseDouble(keyValue[0]);
                    wordFrequencies.add(new WordFrequency(keyValue[1], count));
                }
            }
        }

        Collections.sort(wordFrequencies);
        wordFrequencies = wordFrequencies.subList(0, 300);

        final Dimension dimension = new Dimension(600, 600);
        final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
        wordCloud.setPadding(2);
        wordCloud.setBackground(new CircleBackground(300));
        wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
        wordCloud.setFontScalar(new SqrtFontScalar(8, 50));
        wordCloud.build(wordFrequencies);
        wordCloud.writeToFile(LambdaController.NORMALIZED_CLOUD_PATH + removeExtension(documentWcPath.getName()) + "-normalized.png");
    }

    public static void allTfIdf() throws IOException, ClassNotFoundException, InterruptedException {
        long now = System.currentTimeMillis();

        // tfIdf Job

        BasicConfigurator.configure();                    // Log4j Config oder ConfigFile in Resources Folder
        System.setProperty("hadoop.home.dir", "/");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "allTfIdf");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(AllTfIdfMapper.class);
        job.setReducerClass(TfIdfReducer.class);
        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(FileInputFormat.class);

        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(LambdaController.INVERSE_DOCUMENT_FREQUENCIES_PATH));

        File[] folders = new File(LambdaController.TERM_FREQUENCIES_PATH).listFiles();
        org.apache.hadoop.fs.Path folderPath;
        ArrayList<String> documentNames = new ArrayList<>();
        for (File folder : folders) {
            if (folder.isDirectory()) {
                folderPath = new org.apache.hadoop.fs.Path(folder.getAbsolutePath());
                FileInputFormat.addInputPath(job, folderPath);
                documentNames.add(folder.getName());
                System.out.println("Adding folder: " + folder.getName() + " at: " + folderPath);
            } else {
                System.out.println("FOUND NON FOLDER FILE: " + folder.getName());
            }
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new ObjectOutputStream(out).writeObject(documentNames.toArray());
        String serializedDocumentNames = new String(Hex.encodeHex(out.toByteArray()));

        conf.set("documentNames", serializedDocumentNames);

        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("/tmp/all-tfIdf-output-" + now));

        job.waitForCompletion(true);

        // Job 2 - switch and sort
        /*

        job = Job.getInstance(conf, "switchAndSort");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(DoubleSwitchMapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);

        job.setSortComparatorClass(MyDescendingDoubleComparator.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path("/tmp/tfIdf-output-" + now));

        deleteDirectoryStream(new File(LambdaController.TD_IDF_PATH + removeExtension(documentWcPath.getName())).toPath());
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(LambdaController.TD_IDF_PATH + removeExtension(documentWcPath.getName()));

        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);


        List<WordFrequency> wordFrequencies = new ArrayList<WordFrequency>();

        File[] outputFiles = new File(outputPath.toString()).listFiles();
        for (int i = 0; i < outputFiles.length; i++) {
            String name = outputFiles[i].getName();
            if (outputFiles[i].getName().startsWith("part-r-")) {
                FileInputStream fileInputStream = null;
                byte[] bFile = new byte[(int) outputFiles[i].length()];

                //convert file into array of bytes
                fileInputStream = new FileInputStream(outputFiles[i]);
                fileInputStream.read(bFile);
                fileInputStream.close();
                String[] bString = new String(bFile, StandardCharsets.UTF_8).split("\n");
                String[] keyValue;
                for (String row : bString) {
                    keyValue = row.split("\t");
                    int count = (int) Double.parseDouble(keyValue[0]);
                    wordFrequencies.add(new WordFrequency(keyValue[1], count));
                }
            }
        }

        Collections.sort(wordFrequencies);
        wordFrequencies = wordFrequencies.subList(0, 300);

        final Dimension dimension = new Dimension(600, 600);
        final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
        wordCloud.setPadding(2);
        wordCloud.setBackground(new CircleBackground(300));
        wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
        wordCloud.setFontScalar(new SqrtFontScalar(8, 50));
        wordCloud.build(wordFrequencies);
        wordCloud.writeToFile(LambdaController.NORMALIZED_CLOUD_PATH + removeExtension(documentWcPath.getName()) + "-normalized.png");

         */
    }

    public static List<WordFrequency> wordFrequency(Path path) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();                    // Log4j Config oder ConfigFile in Resources Folder
        System.setProperty("hadoop.home.dir", "/");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);
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
        job.setMapperClass(IntSwitchMapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);

        job.setSortComparatorClass(MyDescendingIntComparator.class);
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

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Pattern pattern = Pattern.compile("(\\b[^\\s\\d-.:]+\\b)");
            Matcher matcher = pattern.matcher(value.toString());
            while (matcher.find()) {
                word.set(value.toString().substring(matcher.start(), matcher.end()).toLowerCase());
                context.write(word, one);
            }
        }
    }

    public static class DocumentFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            word.set(keyValue[0]);
            context.write(word, one);
        }
    }

    public static class InverseDocumentFrequencyMapper extends Mapper<Text, IntWritable, Text, DoubleWritable> {

        DoubleWritable inverseDocumentFrequency = new DoubleWritable();

        public void map(Text word, IntWritable documentFrequency, Context context) throws IOException, InterruptedException {
            int numberOfDocuments = context.getConfiguration().getInt("numberOfDocuments", 0);
            inverseDocumentFrequency.set(Math.log((double) numberOfDocuments / documentFrequency.get()));
            context.write(word, inverseDocumentFrequency);
        }
    }

    public static class TfIdfMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text word = new Text();
        private DoubleWritable value = new DoubleWritable();

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {
            String[] keyValue = row.toString().split("\t");
            word.set(keyValue[0]);
            value.set(Double.parseDouble(keyValue[1]));
            context.write(word, value);
        }
    }

    public static class TfIdfReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text word, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<DoubleWritable> valuesIterator = values.iterator();
            DoubleWritable first = valuesIterator.next();

            if (valuesIterator.hasNext()) {
                result.set(first.get() * valuesIterator.next().get());
            } else {
                result.set(0);
            }

            context.write(word, result);
        }
    }

    public static class AllTfIdfMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text word = new Text();
        private DoubleWritable value = new DoubleWritable();

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();

            String[] keyValue = row.toString().split("\t");

            if(LambdaController.INVERSE_DOCUMENT_FREQUENCIES_PATH.startsWith(fileName)){
                ByteArrayInputStream in = null;
                try {
                    in = new ByteArrayInputStream(Hex.decodeHex(context.getConfiguration().get("documentNames", null).toCharArray()));
                } catch (DecoderException e) {
                    e.printStackTrace();
                }
                String[] documentNames = new String[0];
                try {
                    documentNames = (String[]) new ObjectInputStream(in).readObject();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                for(String documentName : documentNames){
                    word.set(documentName + "," + keyValue[0]);
                    value.set(Double.parseDouble(keyValue[1]));
                    context.write(word, value);
                }
            }else{
                word.set(fileName + "," + keyValue[0]);
                value.set(Double.parseDouble(keyValue[1]));
                context.write(word, value);
            }

        }
    }


    public static class IntSwitchMapper extends Mapper<Text, IntWritable, IntWritable, Text> {

        public void map(Text word, IntWritable count, Context context) throws IOException, InterruptedException {
            context.write(count, word);
        }

    }

    public static class DoubleSwitchMapper extends Mapper<Text, DoubleWritable, DoubleWritable, Text> {

        public void map(Text word, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(value, word);
        }

    }

    public static class NormalizeMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private static DoubleWritable value = new DoubleWritable();

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {

            // Row parsing
            String[] keyValue = row.toString().split("\t");
            int count = Integer.parseInt(keyValue[0]);
            Text word = new Text(keyValue[1]);

            int mostFrequentWordFrequency = context.getConfiguration().getInt("mostFrequentWordFrequency", 0);

            value.set(((double) count / mostFrequentWordFrequency));
            context.write(word, value);
        }
    }
}

class MyDescendingIntComparator extends WritableComparator {
    public MyDescendingIntComparator() {
        super(IntWritable.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        return super.compare(a, b) * (-1);
    }
}

class MyDescendingDoubleComparator extends WritableComparator {
    public MyDescendingDoubleComparator() {
        super(DoubleWritable.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        return super.compare(a, b) * (-1);
    }
}