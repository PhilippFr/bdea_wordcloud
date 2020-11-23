package de.hs_mannheim.informatik.lambda.controller;

import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.CircleBackground;
import com.kennycason.kumo.font.scale.SqrtFontScalar;
import com.kennycason.kumo.nlp.FrequencyAnalyzer;
import com.kennycason.kumo.palette.ColorPalette;
import de.hs_mannheim.informatik.lambda.hadoop.TagCloudNormalizer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.io.FilenameUtils.removeExtension;

@Controller
public class LambdaController {
    public final static String NON_NORMALIZED_CLOUD_PATH = "non-normalized-tagclouds/";
    public final static String NORMALIZED_CLOUD_PATH = "normalized-tagclouds/";
    public final static String DOCUMENTS_PATH = "documents/";
    public final static String TERM_FREQUENCIES_PATH = "term-frequencies/";
    public final static String INVERSE_DOCUMENT_FREQUENCIES_PATH = "inverse-document-frequencies/";
    public final static String SINGLE_TF_IDF_PATH = "single-tf-idf/";
    public final static String ALL_TF_IDF_PATH = "all-tf-idf/";
    public final static int NUM_REDUCE_TASKS = 1;

    @GetMapping("/createNormalizedTagClouds")
    public String createNormalizedTagClouds(Model model) throws InterruptedException, IOException, ClassNotFoundException {
        System.out.println("WORKING WORKING WORKING");
        createNormalizedTagClouds();

        model.addAttribute("nonNormalizedTagClouds", listTagClouds(NON_NORMALIZED_CLOUD_PATH));
        model.addAttribute("normalizedTagClouds", listTagClouds(NORMALIZED_CLOUD_PATH));
        model.addAttribute("nonNormalizedTagCloudsPath", NON_NORMALIZED_CLOUD_PATH);
        model.addAttribute("normalizedTagCloudsPath", NORMALIZED_CLOUD_PATH);

        return "upload";
    }

    @GetMapping("/upload")
    public String forward(Model model) {
        model.addAttribute("nonNormalizedTagClouds", listTagClouds(NON_NORMALIZED_CLOUD_PATH));
        model.addAttribute("normalizedTagClouds", listTagClouds(NORMALIZED_CLOUD_PATH));
        model.addAttribute("nonNormalizedTagCloudsPath", NON_NORMALIZED_CLOUD_PATH);
        model.addAttribute("normalizedTagCloudsPath", NORMALIZED_CLOUD_PATH);

        return "upload";
    }

    @PostMapping("/upload")
    public String handleFileUpload(@RequestParam("file") MultipartFile file, Model model) {
        try {
            model.addAttribute("message", "Datei erfolgreich hochgeladen: " + file.getOriginalFilename());
            saveDocument(file);

            createNonNormalizedTagCloud(removeExtension(file.getOriginalFilename()), new String(file.getBytes()));
            TagCloudNormalizer.termFrequencies(Paths.get(DOCUMENTS_PATH + file.getOriginalFilename()));

            model.addAttribute("nonNormalizedTagClouds", listTagClouds(NON_NORMALIZED_CLOUD_PATH));
            model.addAttribute("normalizedTagClouds", listTagClouds(NORMALIZED_CLOUD_PATH));
            model.addAttribute("nonNormalizedTagCloudsPath", NON_NORMALIZED_CLOUD_PATH);
            model.addAttribute("normalizedTagCloudsPath", NORMALIZED_CLOUD_PATH);
        } catch (Exception e) {
            e.printStackTrace();
            model.addAttribute("message", "Da gab es einen Fehler: " + e.getMessage());
        }

        return "upload";
    }

    private void saveDocument(MultipartFile file) throws Exception {
        // Get the file and save it somewhere
        File directory = new File(DOCUMENTS_PATH);
        if (!directory.exists()) {
            directory.mkdir();
        }
        byte[] bytes = file.getBytes();
        Path path = Paths.get(DOCUMENTS_PATH + file.getOriginalFilename());
        Files.write(path, bytes);
    }

    private String[] listTagClouds(String path) {
        File[] files = new File(path).listFiles();
        String[] clouds = new String[files.length];
        String name;

        for (int i = 0; i < files.length; i++) {
            name = files[i].getName();
            if (files[i].getName().endsWith(".png"))
                clouds[i] = name;
        }

        return clouds;
    }

    private void createNormalizedTagClouds() throws IOException, InterruptedException, ClassNotFoundException {
        TagCloudNormalizer.inverseDocumentFrequencies();
        TagCloudNormalizer.allTfIdf();
        TagCloudNormalizer.globalTfIdf();
    }

    private void createNonNormalizedTagCloud(String filename, String content) throws IOException, InterruptedException, ClassNotFoundException {
        final FrequencyAnalyzer frequencyAnalyzer = new FrequencyAnalyzer();
        frequencyAnalyzer.setWordFrequenciesToReturn(300);
        frequencyAnalyzer.setMinWordLength(4);

        List<String> texts = new ArrayList<>();
        texts.add(content);
        final List<WordFrequency> wordFrequencies = frequencyAnalyzer.load(texts);

        final Dimension dimension = new Dimension(600, 600);
        final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
        wordCloud.setPadding(2);
        wordCloud.setBackground(new CircleBackground(300));
        wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
        wordCloud.setFontScalar(new SqrtFontScalar(8, 50));
        wordCloud.build(wordFrequencies);
        wordCloud.writeToFile(NON_NORMALIZED_CLOUD_PATH + filename + ".png");
    }
}
