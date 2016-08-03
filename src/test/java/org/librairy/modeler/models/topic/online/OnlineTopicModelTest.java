package org.librairy.modeler.models.topic.online;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import es.cbadenes.lab.test.IntegrationTest;
import es.upm.oeg.epnoi.matching.metrics.distance.JensenShannonDivergence;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.*;
import org.apache.spark.mllib.linalg.Vector;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.model.domain.relations.Bundles;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Item;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.BagOfWords;
import org.librairy.modeler.lda.helper.SparkHelper;
import org.librairy.storage.UDM;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created on 13/04/16:
 *
 * @author cbadenes
 */
public class OnlineTopicModelTest extends AbstractEvaluation{

    private static final Logger LOG = LoggerFactory.getLogger(OnlineTopicModelTest.class);


    @Test
    public void onlineLDA() throws IOException {

        // Test Settings

        // -> Sample
        int TRAINING_SIZE       = 10;  // number of documents used to build the model
        int TESTING_SIZE        = 10;  // number of documents used to evaluate the model
        int TESTING_ITERATIONS  = 10;   // number of evaluations for the same model
        // -> LDA
        Integer MAX_ITERATIONS  =   100;
        Integer NUM_TOPICS      =   5;    // number of clusters
        Double ALPHA            =  -1.0;  // document concentration
        Double BETA             =  -1.0;  // topic concentration
        // -> Online Optimizer
        Double TAU              =   1.0;  // how downweight early iterations
        Double KAPPA            =   0.5;  // how quickly old information is forgotten
        Double BATCH_SIZE_RATIO  =   Math.min(1.0,2.0 / MAX_ITERATIONS + 1.0 / TRAINING_SIZE);  // how many documents
        // are used each iteration

        LOG.info("Test settings: \n"
                +"- Training Size= " + TRAINING_SIZE
                +"- Testing Size= " + TESTING_SIZE
                +"- Max Iterations= " + MAX_ITERATIONS
                +"- Num Topics= " + NUM_TOPICS
                +"- Alpha= " + ALPHA
                +"- Beta= " + BETA
                +"- Tau= " + TAU
                +"- Kappa= " + KAPPA
                +"- Batch Size Ratio= " + BATCH_SIZE_RATIO
        );

        LOG.info
                ("====================================================================================================");
        LOG.info(" STAGE 1: Training the Model");
        LOG.info
                ("====================================================================================================");

        PatentsReferenceModel.TestSample trainingSet = refModel.sampleOf(TRAINING_SIZE,false);
        List<String> uris = trainingSet.getReferences();

        Corpus trainingCorpus = _composeCorpus(uris);
        JavaPairRDD<Long, Vector> trainingBagsOfWords = trainingCorpus.getBagsOfWords().cache();

        // Online LDA Model :: Creation
        OnlineLDAOptimizer onlineLDAOptimizer = new OnlineLDAOptimizer()
                .setMiniBatchFraction(BATCH_SIZE_RATIO)
                .setOptimizeDocConcentration(true)
                .setTau0(TAU)
                .setKappa(KAPPA)
                ;

        LOG.info("Building the model...");
        LDAModel ldaModel = new LDA().
                setAlpha(ALPHA).
                setBeta(BETA).
                setK(NUM_TOPICS).
                setMaxIterations(MAX_ITERATIONS).
                setOptimizer(onlineLDAOptimizer).
                run(trainingBagsOfWords);

        LocalLDAModel localLDAModel = (LocalLDAModel) ldaModel;


        // Online LDA Model :: Description
        LOG.info("## Online LDA Model :: Description");

        LOG.info("Perplexity: " + localLDAModel.logPerplexity(trainingBagsOfWords));
        LOG.info("Likelihood: " + localLDAModel.logLikelihood(trainingBagsOfWords));


        // Output topics. Each is a distribution over words (matching word count vectors)
        LOG.info("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
                + " words):");


        Map<Long,String> vocabularyInverse = trainingCorpus.getVocabulary().entrySet()
                .stream()
                .collect(Collectors.toMap((x->x.getValue()),(y->y.getKey())));

        int index = 0;
        for (Tuple2<int[], double[]> description : ldaModel.describeTopics(10)){

            LOG.info("Topic: " + index++);
            int[] words = description._1;
            double[] density = description._2;

            for (int i=0;i<words.length;i++){
                LOG.info("\t ["+words[i]+"]'" + vocabularyInverse.get(Long.valueOf(""+words[i]))+"': " + density[i] );
            }
        }


        LOG.info
                ("====================================================================================================");
        LOG.info(" STAGE 2: Testing the Model");
        LOG.info
                ("====================================================================================================");

        List<Double> accumulatedRates = new ArrayList<>();
        for (int i= 0; i< TESTING_ITERATIONS; i++){
            // Online LDA Model :: Inference
            LOG.info("## Online LDA Model :: Inference");
            PatentsReferenceModel.TestSample testingSet = refModel.sampleOf(TESTING_SIZE,true);
            List<String> testUris = testingSet.all;

            Corpus testingCorpus = _composeCorpus(testUris,trainingCorpus.getVocabulary());
            JavaPairRDD<Long, Vector> testingBagsOfWords = testingCorpus.getBagsOfWords().cache();


            // Online LDA Model :: Similarity based on Jensen-Shannon Divergence
            LOG.info("## Online LDA Model :: Similarity");

            JavaPairRDD<Long, Vector> topicDistributions = localLDAModel.topicDistributions(testingBagsOfWords);

            Map<Long, String> documents = testingCorpus.getDocuments();

            topicDistributions.collect().forEach(dist -> LOG.info("'" + itemDocCache.getUnchecked(documents.get
                    (dist._1))
                    + "': "+
                    dist._2));

            SimMatrix simMatrix = new SimMatrix();

            List<WeightedPair> similarities = topicDistributions.
                    cartesian(topicDistributions).
                    filter(x -> x._1._1.compareTo(x._2()._1) > 0).
                    map(x -> new WeightedPair(documents.get(x._1._1), documents.get(x._2._1),
                            JensenShannonDivergence
                                    .apply(x._1()._2.toArray(), x._2()._2.toArray()))).
                    collect();

            similarities.forEach(w -> simMatrix.add(w.getWeight(),w.getUri1(),w.getUri2()));

            LOG.info("## Online LDA Model :: Evaluation");
            Map<String,Double> evalByFit = new HashMap();

            testingSet.references.stream().
                    filter(uri -> udm.exists(Resource.Type.DOCUMENT).withUri(uri)).
                    forEach(uri ->{
                        List<String> refSimilars    = refModel.getRefs(uri).stream().map(refUri -> docItemCache
                                .getUnchecked(refUri)).filter(x -> !x.trim().isEmpty()).collect(Collectors.toList());
                        List<String> similars       = simMatrix.getSimilarsTo(docItemCache.getUnchecked(uri)).subList(0, TESTING_SIZE/3);
                        Double fit                  = SortedListMeter.measure(refSimilars,similars);
                        LOG.info("Patent: " + uri + " fit in " + fit + "|| Refs:["+refSimilars.stream().map(x -> itemDocCache.getUnchecked(x)).collect(Collectors.toList())+"]  " +
                                "Similars:["+similars.stream().map(z->itemDocCache.getUnchecked(z)).collect(Collectors.toList())+"]");
                        // Comparison
                        evalByFit.put(uri,fit);
                    });

            Double acumRate = evalByFit.entrySet().stream().map(x -> x.getValue()).reduce((x, y) -> x + y).get()/evalByFit.size();
            LOG.info("Global Fit Rate: " + (acumRate));
            accumulatedRates.add(acumRate);
        }

        LOG.info("Final Accumulated Fit-Rate: " + accumulatedRates.stream().reduce((x,y) -> x+y).get()
                /accumulatedRates.size());

    }


}
