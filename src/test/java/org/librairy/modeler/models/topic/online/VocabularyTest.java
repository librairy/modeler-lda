package org.librairy.modeler.models.topic.online;

import com.google.gson.Gson;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Created on 13/04/16:
 *
 * @author cbadenes
 */
public class VocabularyTest extends AbstractEvaluation{

    private static final Logger LOG = LoggerFactory.getLogger(VocabularyTest.class);

    @Test
    public void checkVocabulary() throws IOException {

        LOG.info("Starting vocabulary test ..");

        FileWriter writer = new FileWriter("src/test/resources/vocabulary.csv");
        writer.append("documents").append(",")
                .append("words").append(",")
                .append("time(secs)").
                append("\n");

        Arrays.asList(new Integer[]{
//                100,
//                500,
//                1000,
//                2000,
//                3000,
//                4000,
//                5000,
//                6000,
//                7000,
//                8000,
//                9000,
//                10000,
                21000,
                22000,
                23000,
                24000,
                25000,
                26000,
                27000,
                28000,
                29000,
                30000,
                31000,
                32000,
                33000,
                34000,
                35000,
                36000,
                37000,
                38000,
                39000,
                40000
        }).forEach(size ->{

            Instant start = Instant.now();
            List<String> corpusUris = trainingSet.getUris().subList(0,size);
            Corpus corpus = _composeCorpus(corpusUris);
            Instant end = Instant.now();

            try {

                Map<String, Long> vocabulary = corpus.getVocabulary();

                writer.append(String.valueOf(size)).append(",")
                            .append(String.valueOf(vocabulary.size())).append(",")
                            .append(String.valueOf(ChronoUnit.SECONDS.between(start,end)))
                            .append("\n");

            } catch (IOException e) {
                LOG.warn("Error writing to csv",e);
            }
            LOG.info("Elapsed time for vocabulary of " +  size + " documents: " + ChronoUnit.MINUTES.between(start,
                    end) + "min " + (ChronoUnit.SECONDS.between(start,end)%60) + "secs");

        });
        writer.flush();
        writer.close();





//        FileWriter writer = new FileWriter("src/test/resources/perplexity-"+topics+".csv");
//
//        writer.append("documents").append(",")
//                .append("topics").append(",")
//                .append("perplexity").append(",")
//                .append("likelihood").append(",")
//                .append("time").append("\n");
//
//
//        try {
//            writer.append(String.valueOf(corpusUris.size())).append(",");
//            writer.append(String.valueOf(topics)).append(",");
//            writer.append(String.valueOf(model.logPerplexity(bow))).append(",");
//            writer.append(String.valueOf(model.logLikelihood(bow))).append(",");
//            writer.append(String.valueOf(endModel-startModel)).append("\n");
//        } catch (IOException e) {
//            LOG.warn("Error writing to csv",e);
//        }
//
//        writer.flush();
//        writer.close();




    }


    @Test
    public void checkWords() throws IOException {

        LOG.info("Starting vocabulary test ..");

        int size = 100;

        FileWriter writer = new FileWriter("src/test/resources/vocabulary-"+size+".csv");
        writer.append("word").append(",")
                .append("times").
                append("\n");


        Instant start = Instant.now();
        List<String> corpusUris = trainingSet.getUris().subList(0,size);
        Corpus corpus = _composeCorpus(corpusUris);
        Instant end = Instant.now();

        try {

            Map<String, Long> vocabulary = corpus.getVocabulary();


            List<String> words = new ArrayList<String>(vocabulary.keySet());

            Collections.sort(words);

            for (String word: words){

                writer.append(word).append(",")
                        .append(String.valueOf(vocabulary.get(word)))
                        .append("\n");
            }


        } catch (IOException e) {
            LOG.warn("Error writing to csv",e);
        }
        LOG.info("Elapsed time for vocabulary of " +  size + " documents: " + ChronoUnit.MINUTES.between(start,
                end) + "min " + (ChronoUnit.SECONDS.between(start,end)%60) + "secs");


        writer.flush();
        writer.close();




    }





}
