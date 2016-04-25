package org.librairy.modeler.models.topic.online;

import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
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

        Corpus corpus = _composeCorpus(trainingSet.getUris().stream().limit(100).collect(Collectors.toList()));

        Map<Integer,Double> perplexityTable = new HashMap();
        Map<Integer,Double> likelihoodTable = new HashMap();
        Map<Integer,Long> timeTable         = new HashMap();


        List<Integer> topics = Arrays.asList(new Integer[]{20, 40, 60, 80, 100, 120, 140, 160, 180, 200});

        topics.forEach(k ->{

            Long start = System.currentTimeMillis();
            LocalLDAModel model = _buildModel(ALPHA, BETA, k, ITERATIONS, corpus);
            Long end = System.currentTimeMillis();

            perplexityTable.put(k,model.logPerplexity(corpus.bagsOfWords));
            likelihoodTable.put(k,model.logLikelihood(corpus.bagsOfWords));
            timeTable.put(k,end-start);
        });

        FileWriter writer = new FileWriter("src/test/resources/perplexity.csv");

        writer.append("topics").append(",").append("perplexity").append(",").append("likelihood").append(",").append
                ("time").append("\n");

        topics.forEach(topic ->{
            try {
                writer.append(String.valueOf(topic)).append(",");
                writer.append(String.valueOf(perplexityTable.get(topic))).append(",");
                writer.append(String.valueOf(likelihoodTable.get(topic))).append(",");
                writer.append(String.valueOf(timeTable.get(topic))).append("\n");
            } catch (IOException e) {
                LOG.warn("Error writing to csv",e);
            }
        });

        writer.flush();
        writer.close();

    }





}
