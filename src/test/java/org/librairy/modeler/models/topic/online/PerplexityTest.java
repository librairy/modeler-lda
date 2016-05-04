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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 13/04/16:
 *
 * @author cbadenes
 */
public class PerplexityTest extends AbstractEvaluation{

    private static final Logger LOG = LoggerFactory.getLogger(PerplexityTest.class);

    @Test
    public void perplexity20() throws IOException {
        perplexity(20);
    }

    @Test
    public void perplexity40() throws IOException {
        perplexity(40);
    }

    @Test
    public void perplexity60() throws IOException {
        perplexity(60);
    }

    @Test
    public void perplexity80() throws IOException {
        perplexity(80);
    }

    @Test
    public void perplexity100() throws IOException {
        perplexity(100);
    }

    @Test
    public void perplexity120() throws IOException {
        perplexity(120);
    }

    @Test
    public void perplexity140() throws IOException {
        perplexity(140);
    }

    @Test
    public void perplexity160() throws IOException {
        perplexity(160);
    }

    @Test
    public void perplexity180() throws IOException {
        perplexity(180);
    }

    @Test
    public void perplexity200() throws IOException {
        perplexity(200);
    }


    public void perplexity(int topics) throws IOException {

        LOG.info("Starting perplexity test ..");
        Instant start = Instant.now();

        List<String> corpusUris = trainingSet.getUris().subList(0,50);

        Corpus corpus = _composeCorpus(corpusUris);
        JavaPairRDD<Long, Vector> bow = corpus.bagsOfWords.persist(StorageLevel.MEMORY_ONLY());

        Long startModel = System.currentTimeMillis();
        LocalLDAModel model = _buildModel(ALPHA, BETA, topics, ITERATIONS, corpus);
        Long endModel = System.currentTimeMillis();

        model.save(sparkHelper.getSc().sc(),"src/test/resources/model-"+topics);


        Gson gson = new Gson();

        // Serialize.
        String json = gson.toJson(corpus.getVocabulary());
        FileWriter jsonFile = new FileWriter("src/test/resources/vocabulary-"+topics+".json");
        jsonFile.append(json); // {"key1":"value1","key2":"value2","key3":"value3"}
        jsonFile.flush();
        jsonFile.close();

        // Deserialize.
//        Map<String, String> map2 = gson.fromJson(json, new TypeToken<Map<String, String>>() {}.getType());
//        System.out.println(map2); // {key1=value1, key2=value2, key3=value3}


        FileWriter writer = new FileWriter("src/test/resources/perplexity-"+topics+".csv");

        writer.append("documents").append(",")
                .append("topics").append(",")
                .append("perplexity").append(",")
                .append("likelihood").append(",")
                .append("time").append("\n");


        try {
            writer.append(String.valueOf(corpusUris.size())).append(",");
            writer.append(String.valueOf(topics)).append(",");
            writer.append(String.valueOf(model.logPerplexity(bow))).append(",");
            writer.append(String.valueOf(model.logLikelihood(bow))).append(",");
            writer.append(String.valueOf(endModel-startModel)).append("\n");
        } catch (IOException e) {
            LOG.warn("Error writing to csv",e);
        }

        writer.flush();
        writer.close();


        Instant end = Instant.now();
        LOG.info("Elapsed time for perplexity-test: " + ChronoUnit.MINUTES.between(start,end) + "min " + ChronoUnit
                .SECONDS.between(start,end) + "secs");

    }





}
