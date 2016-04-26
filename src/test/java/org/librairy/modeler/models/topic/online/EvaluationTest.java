package org.librairy.modeler.models.topic.online;

import es.upm.oeg.epnoi.matching.metrics.distance.JensenShannonDivergence;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 13/04/16:
 *
 * @author cbadenes
 */
public class EvaluationTest extends AbstractEvaluation{

    private static final Logger LOG = LoggerFactory.getLogger(EvaluationTest.class);

    @Test
    public void evaluation() throws IOException {

        Corpus trainingCorpus = _composeCorpus(trainingSet.getUris());

        File modelFile = new File("src/test/resources/model");
        LocalLDAModel model = null;
        if (!modelFile.exists()){

            int k = 100;
            model = _buildModel(ALPHA, BETA, k, ITERATIONS, trainingCorpus);
            LOG.info("Saving model to file: " + modelFile.getAbsolutePath());
            model.save(sparkHelper.getSc().sc(),modelFile.getAbsolutePath());
            LOG.info("Model saved in: " + modelFile.getAbsolutePath());
        }else{
            LOG.info("model load from local file: " + modelFile.getAbsolutePath());
            model = LocalLDAModel.load(sparkHelper.getSc().sc(),modelFile.getAbsolutePath());
        }


        Corpus testCorpus = _composeCorpus(testSet.getUris(), trainingCorpus.getVocabulary());
        JavaPairRDD<Long, Vector> bow = testCorpus.bagsOfWords.persist(StorageLevel.MEMORY_AND_DISK());

        Double similarityThreshold = 0.5;


        LOG.info("## Online LDA Model :: Similarity");

        JavaPairRDD<Long, Vector> topicDistributions = model.topicDistributions(bow);
        JavaPairRDD<Long, Vector> distributions = topicDistributions.persist(StorageLevel.MEMORY_AND_DISK());

        Map<Long, String> documents = testCorpus.getDocuments();

        SimMatrix simMatrix = new SimMatrix();

        Accumulator<SimMatrix> res = sparkHelper.getSc().accumulator(new SimMatrix(), null);

        List<WeightedPair> similarities = distributions.
                cartesian(distributions).
                filter(x -> x._1._1.compareTo(x._2()._1) > 0).
                map(x -> new WeightedPair(documents.get(x._1._1), documents.get(x._2._1),
                        JensenShannonDivergence
                                .apply(x._1()._2.toArray(), x._2()._2.toArray()))).
                collect();

        similarities.forEach(w -> simMatrix.add(w.getWeight(),w.getUri1(),w.getUri2()));


        LOG.info("## Online LDA Model :: Evaluation");

        long TP = 0;
        long FP = 0;
        long FN = 0;
        long TN = 0;

        FileWriter writer = new FileWriter("src/test/resources/evaluation.csv");

        writer.append("patent").append(",").append("tp").append(",").append("fp").append(",").append
                ("fn").append(",").append("tn").append("\n");

        for (String key : simMatrix.matrix.keySet()){
            List<String> citedPatents   = refModel.getRefs(key);
            List<String> similarPatents = new ArrayList<String>();
            List<String> noSimilarPatents = new ArrayList<String>();

            simMatrix.getPatentsFrom(key).entrySet().stream().forEach(entry -> {
                if (entry.getValue()>= similarityThreshold) similarPatents.add(entry.getKey());
                else noSimilarPatents.add(entry.getKey());
            });


            long partialTP  = citedPatents.stream().filter(uri -> similarPatents.contains(uri)).count();
            writer.append(String.valueOf(partialTP)).append(",");
            long partialFP  = similarPatents.stream().filter(uri -> !citedPatents.contains(uri)).count();
            writer.append(String.valueOf(partialFP)).append(",");
            long partialFN  = citedPatents.stream().filter(uri -> !similarPatents.contains(uri)).count();
            writer.append(String.valueOf(partialFN)).append(",");
            long partialTN  = noSimilarPatents.stream().filter(uri -> !citedPatents.contains(uri)).count();
            writer.append(String.valueOf(partialTN)).append("\n");

            LOG.info("[" + key + " | TP:"+partialTP+" | FP:" + partialFP + " | FN:" + partialFN + " | TN:" + partialTN);

            TP += partialTP;
            FP += partialFP;
            FN += partialFN;
            TN += partialTN;
        }

        writer.flush();
        writer.close();

        FileWriter writer2 = new FileWriter("src/test/resources/evaluation-aggregated.csv");
        writer2.append("tp").append(",")
                .append("fp").append(",")
                .append("fn").append(",")
                .append("tn").append(",")
                .append("N+").append(",")
                .append("N-").append(",")
                .append("recall").append(",")
                .append("precision").append(",")
                .append("\n");

        long positiveClassifications = TP + FP;
        long negativeClassifications = TP + FN;

        LOG.info("Evaluation: ");
        LOG.info("TP: " + TP);
        writer2.append(String.valueOf(TP)).append(",");
        LOG.info("FP: " + FP);
        writer2.append(String.valueOf(FP)).append(",");
        LOG.info("FN: " + FN);
        writer2.append(String.valueOf(FN)).append(",");
        LOG.info("TN: " + TN);
        writer2.append(String.valueOf(TN)).append(",");
        LOG.info("N+: " + positiveClassifications);
        writer2.append(String.valueOf(positiveClassifications)).append(",");
        LOG.info("N-: " + negativeClassifications);
        writer2.append(String.valueOf(negativeClassifications)).append(",");

        double recall       = Double.valueOf(TP) / Double.valueOf(negativeClassifications);
        double precision    = Double.valueOf(TP) / Double.valueOf(positiveClassifications);
        LOG.info("Recall: " + recall);
        writer2.append(String.valueOf(recall)).append(",");
        LOG.info("Precision: " + precision);
        writer2.append(String.valueOf(precision)).append("\n");

        writer2.flush();
        writer2.close();

    }





}
