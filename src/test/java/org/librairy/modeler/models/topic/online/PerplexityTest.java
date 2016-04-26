package org.librairy.modeler.models.topic.online;

import org.apache.spark.mllib.clustering.LocalLDAModel;
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
    public void perplexity() throws IOException {

        LOG.info("Starting perplexity test ..");
        Instant start = Instant.now();


        List<String> corpusUris = trainingSet.getUris();

        Corpus corpus = _composeCorpus(corpusUris);

        Map<Integer,Double> perplexityTable = new HashMap();
        Map<Integer,Double> likelihoodTable = new HashMap();
        Map<Integer,Long> timeTable         = new HashMap();


        List<Integer> topics = Arrays.asList(new Integer[]{20, 40, 60, 80, 100, 120, 140, 160, 180, 200});

//        List<Integer> topics = Arrays.asList(new Integer[]{20});

        for (Integer k : topics){
            Long startModel = System.currentTimeMillis();
            LocalLDAModel model = _buildModel(ALPHA, BETA, k, ITERATIONS, corpus);
            Long endModel = System.currentTimeMillis();

            perplexityTable.put(k,model.logPerplexity(corpus.bagsOfWords));
            likelihoodTable.put(k,model.logLikelihood(corpus.bagsOfWords));
            timeTable.put(k,endModel-startModel);
        }



        FileWriter writer = new FileWriter("src/test/resources/perplexity.csv");

        writer.append("documents").append(",")
                .append("topics").append(",")
                .append("perplexity").append(",")
                .append("likelihood").append(",")
                .append("time").append("\n");


        for(Integer topic: topics){
            try {
                writer.append(String.valueOf(corpusUris.size())).append(",");
                writer.append(String.valueOf(topic)).append(",");
                writer.append(String.valueOf(perplexityTable.get(topic))).append(",");
                writer.append(String.valueOf(likelihoodTable.get(topic))).append(",");
                writer.append(String.valueOf(timeTable.get(topic))).append("\n");
            } catch (IOException e) {
                LOG.warn("Error writing to csv",e);
            }
        }

        writer.flush();
        writer.close();


        Instant end = Instant.now();
        LOG.info("Elapsed time for perplexity-test: " + ChronoUnit.MINUTES.between(start,end) + "min " + ChronoUnit
                .SECONDS.between(start,end) + "secs");

    }





}
