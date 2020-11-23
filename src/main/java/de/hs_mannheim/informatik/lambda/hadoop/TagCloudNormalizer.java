package de.hs_mannheim.informatik.lambda.hadoop;

import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.CircleBackground;
import com.kennycason.kumo.font.scale.SqrtFontScalar;
import com.kennycason.kumo.palette.ColorPalette;
import de.hs_mannheim.informatik.lambda.controller.LambdaController;
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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.io.FilenameUtils.removeExtension;

public class TagCloudNormalizer {

    public static void deleteDirectoryStream(Path path) throws IOException {
        File file = new File(path.toString());
        if (file.isDirectory()) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    public static void termFrequencies(Path path) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();                    // Log4j Config oder ConfigFile in Resources Folder
        System.setProperty("hadoop.home.dir", "/");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "wordCount");
        job.setJarByClass(TagCloudNormalizer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(path.toAbsolutePath().toString()));

        // delete old term frequencies folder for this document
        deleteDirectoryStream(new File(LambdaController.TERM_FREQUENCIES_PATH + removeExtension(path.getFileName().toString())).toPath());

        org.apache.hadoop.fs.Path termFrequenciesPath = new org.apache.hadoop.fs.Path(LambdaController.TERM_FREQUENCIES_PATH + removeExtension(path.getFileName().toString()));
        FileOutputFormat.setOutputPath(job, termFrequenciesPath);

        job.waitForCompletion(true);
    }

    public static void inverseDocumentFrequencies() throws IOException, ClassNotFoundException, InterruptedException {
        long now = System.currentTimeMillis();

        BasicConfigurator.configure();                    // Log4j Config oder ConfigFile in Resources Folder
        System.setProperty("hadoop.home.dir", "/");

        Configuration conf = new Configuration();

        // -----------------------> Job 1 - Count in how many documents the word is appearing

        Job job = Job.getInstance(conf, "documentFrequency");
        job.setJarByClass(TagCloudNormalizer.class);
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

        SequenceFileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("/tmp/df-output-" + now));

        job.waitForCompletion(true);

        // -----------------------> Job 2 - Calculate the inverse document frequency for every term in the corpus

        job = Job.getInstance(conf, "inverseDocumentFrequency");
        job.setJarByClass(TagCloudNormalizer.class);
        job.setMapperClass(InverseDocumentFrequencyMapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path("/tmp/df-output-" + now));

        // delete old inverse document frequency result
        deleteDirectoryStream(new File(LambdaController.INVERSE_DOCUMENT_FREQUENCIES_PATH).toPath());

        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(LambdaController.INVERSE_DOCUMENT_FREQUENCIES_PATH));

        job.waitForCompletion(true);
    }

    public static void singleTfIdf(org.apache.hadoop.fs.Path termFrequenciesPath) throws IOException, ClassNotFoundException, InterruptedException {
        long now = System.currentTimeMillis();

        BasicConfigurator.configure();                    // Log4j Config oder ConfigFile in Resources Folder
        System.setProperty("hadoop.home.dir", "/");

        Configuration conf = new Configuration();

        // -----------------------> Job 1 - calculate tfidf

        Job job = Job.getInstance(conf, "singleTfIdf");
        job.setJarByClass(TagCloudNormalizer.class);
        job.setMapperClass(TfIdfMapper.class);
        job.setReducerClass(TfIdfReducer.class);
        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, termFrequenciesPath);
        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(LambdaController.INVERSE_DOCUMENT_FREQUENCIES_PATH));

        SequenceFileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("/tmp/tfIdf-output-" + now));

        job.waitForCompletion(true);

        // -----------------------> Job 2 - switch and sort

        job = Job.getInstance(conf, "switchAndSort");
        job.setJarByClass(TagCloudNormalizer.class);
        job.setMapperClass(DoubleSwitchMapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);
        job.setSortComparatorClass(MyDescendingDoubleComparator.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path("/tmp/tfIdf-output-" + now));

        // delete old tfidf result
        deleteDirectoryStream(new File(LambdaController.SINGLE_TF_IDF_PATH + removeExtension(termFrequenciesPath.getName())).toPath());
        org.apache.hadoop.fs.Path singleTfIdfOutputPath = new org.apache.hadoop.fs.Path(LambdaController.SINGLE_TF_IDF_PATH + removeExtension(termFrequenciesPath.getName()));

        FileOutputFormat.setOutputPath(job, singleTfIdfOutputPath);

        job.waitForCompletion(true);

        // -----------------------> create tag cloud from tfidf of one document

        List<WordFrequency> wordFrequencies = new ArrayList<>();

        File[] outputFiles = new File(singleTfIdfOutputPath.toString()).listFiles();
        for (File outputFile : outputFiles) {
            if (outputFile.getName().startsWith("part-r-")) {
                FileInputStream fileInputStream;
                byte[] bFile = new byte[(int) outputFile.length()];

                // convert file into array of bytes
                fileInputStream = new FileInputStream(outputFile);
                fileInputStream.read(bFile);
                fileInputStream.close();
                String[] bString = new String(bFile, StandardCharsets.UTF_8).split("\n");
                String[] keyValue;
                int count;
                for (String row : bString) {
                    keyValue = row.split("\t");
                    count = (int) Double.parseDouble(keyValue[0]);
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
        wordCloud.writeToFile(LambdaController.NORMALIZED_CLOUD_PATH + removeExtension(termFrequenciesPath.getName()) + "-normalized.png");
    }

    public static void allTfIdf() throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();                    // Log4j Config oder ConfigFile in Resources Folder
        System.setProperty("hadoop.home.dir", "/");

        Configuration conf = new Configuration();

        File[] folders = new File(LambdaController.TERM_FREQUENCIES_PATH).listFiles();
        ArrayList<String> documentNames = new ArrayList<>();
        for (File folder : folders) {
            if (folder.isDirectory()) {
                documentNames.add(folder.getName());
            } else {
                System.out.println("FOUND NON FOLDER FILE: " + folder.getName());
            }
        }

        String[] documentNamesArray = documentNames.toArray(new String[0]);
        conf.setStrings("documentNames", documentNamesArray);

        // -----------------------> Job 1 - calculate tfidf for all documents

        Job job = Job.getInstance(conf, "allTfIdf");
        job.setJarByClass(TagCloudNormalizer.class);
        job.setMapperClass(AllTfIdfMapper.class);
        job.setReducerClass(TfIdfReducer.class);
        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(LambdaController.INVERSE_DOCUMENT_FREQUENCIES_PATH));

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

        // delete old tfidf result
        deleteDirectoryStream(new File(LambdaController.ALL_TF_IDF_PATH).toPath());
        org.apache.hadoop.fs.Path allTfIdfOutputPath = new org.apache.hadoop.fs.Path(LambdaController.ALL_TF_IDF_PATH);
        FileOutputFormat.setOutputPath(job, allTfIdfOutputPath);

        job.waitForCompletion(true);

        // -----------------------> create tag cloud from tfidf of each document

        HashMap<String, List<WordFrequency>> documentsIdIdf = new HashMap<>();

        File[] outputFiles = new File(allTfIdfOutputPath.toString()).listFiles();
        for (File outputFile : outputFiles) {
            if (outputFile.getName().startsWith("part-r-")) {
                FileInputStream fileInputStream;
                byte[] bFile = new byte[(int) outputFile.length()];

                // convert file into array of bytes
                fileInputStream = new FileInputStream(outputFile);
                fileInputStream.read(bFile);
                fileInputStream.close();
                String[] bString = new String(bFile, StandardCharsets.UTF_8).split("\n");
                for (String row : bString) {
                    String[] parts = row.split("\t");
                    String documentName = parts[0].split(";")[0];
                    String word = parts[0].split(";")[1];
                    int value = (int) Double.parseDouble(parts[1]);
                    if(documentsIdIdf.get(documentName) == null){
                        documentsIdIdf.put(documentName, new ArrayList<>());
                    }
                    documentsIdIdf.get(documentName).add(new WordFrequency(word, value));
                }
            }
        }

        for(String documentNameTdIdf: documentsIdIdf.keySet()){
            List<WordFrequency> currentDocumentList = documentsIdIdf.get(documentNameTdIdf);

            Collections.sort(currentDocumentList);
            currentDocumentList = currentDocumentList.subList(0, 300);

            final Dimension dimension = new Dimension(600, 600);
            final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
            wordCloud.setPadding(2);
            wordCloud.setBackground(new CircleBackground(300));
            wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
            wordCloud.setFontScalar(new SqrtFontScalar(8, 50));
            wordCloud.build(currentDocumentList);
            wordCloud.writeToFile(LambdaController.NORMALIZED_CLOUD_PATH + removeExtension(documentNameTdIdf) + "-normalized.png");
        }

    }

    public static void globalTfIdf() throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();                    // Log4j Config oder ConfigFile in Resources Folder
        System.setProperty("hadoop.home.dir", "/");

        Configuration conf = new Configuration();

        // -----------------------> Job 1 - calculate term frequencies over all documents

        Job job = Job.getInstance(conf, "globalTermFrequency");
        job.setJarByClass(TagCloudNormalizer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(LambdaController.NUM_REDUCE_TASKS);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        File[] folder = new File(LambdaController.DOCUMENTS_PATH).listFiles();
        org.apache.hadoop.fs.Path filePath;
        ArrayList<String> documentNames = new ArrayList<>();
        for (File file : folder) {
            filePath = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
            FileInputFormat.addInputPath(job, filePath);
            documentNames.add(file.getName());
            System.out.println("Adding file: " + file.getName() + " at: " + filePath);
        }

        // delete old term frequencies folder for this document
        deleteDirectoryStream(new File(LambdaController.TERM_FREQUENCIES_PATH + "global").toPath());

        org.apache.hadoop.fs.Path termFrequenciesPath = new org.apache.hadoop.fs.Path(LambdaController.TERM_FREQUENCIES_PATH + "global");
        FileOutputFormat.setOutputPath(job, termFrequenciesPath);

        job.waitForCompletion(true);

        // -----------------------> Job 2 - calculate tfidf for global term frequencies

       singleTfIdf(termFrequenciesPath);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Pattern pattern = Pattern.compile("(\\b[^\\s\\d-.:]+\\b)");
            Matcher matcher = pattern.matcher(value.toString());
            while (matcher.find()) {
                word.set(value.toString().substring(matcher.start(), matcher.end()).toLowerCase());
                context.write(word, ONE);
            }
        }
    }

    public static class DocumentFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            word.set(keyValue[0]);
            context.write(word, ONE);
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
        private final Text word = new Text();
        private final DoubleWritable value = new DoubleWritable();

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {
            String[] keyValue = row.toString().split("\t");
            word.set(keyValue[0]);
            value.set(Double.parseDouble(keyValue[1]));
            context.write(word, value);
        }
    }

    public static class TfIdfReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable result = new DoubleWritable();

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
        private final Text word = new Text();
        private final DoubleWritable value = new DoubleWritable();

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();

            String[] keyValue = row.toString().split("\t");

            if (LambdaController.INVERSE_DOCUMENT_FREQUENCIES_PATH.startsWith(fileName)) {
                String[] documentNames = context.getConfiguration().getStrings("documentNames");

                for (String documentName : documentNames) {
                    word.set(documentName + ";" + keyValue[0]);
                    value.set(Double.parseDouble(keyValue[1]));
                    context.write(word, value);
                }
            } else {
                word.set(fileName + ";" + keyValue[0]);
                value.set(Double.parseDouble(keyValue[1]));
                context.write(word, value);
            }

        }
    }

    public static class DoubleSwitchMapper extends Mapper<Text, DoubleWritable, DoubleWritable, Text> {
        public void map(Text word, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(value, word);
        }
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