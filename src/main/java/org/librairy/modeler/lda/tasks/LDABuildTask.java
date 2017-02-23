/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.clustering.OnlineLDAOptimizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.TopicRow;
import org.librairy.modeler.lda.dao.TopicsDao;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.ComputingKey;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.modeler.lda.optimizers.LDAOptimizer;
import org.librairy.modeler.lda.optimizers.LDAParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class LDABuildTask {

    private static final Logger LOG = LoggerFactory.getLogger(LDABuildTask.class);

    private final ComputingContext context;
    private final ModelingHelper helper;

    public LDABuildTask(ComputingContext context, ModelingHelper helper){
        this.context = context;
        this.helper = helper;
    }


    public TopicModel create(Corpus corpus, Integer maxWords) {

        // Train
        TopicModel model = train(corpus);

        // Persist in File System
        saveToFileSystem(model,corpus.getId());

        // Persist in Data Base
        saveToDataBase(model,corpus.getId(), maxWords);

        return model;
    }

    public void saveToDataBase(TopicModel model, String corpusId, Integer maxWords){


        Broadcast<String[]> broadcastVocabulary = context.getSparkContext().broadcast(model.getVocabModel().vocabulary
                ());


        JavaRDD<Tuple2<int[], double[]>> topics = context.getSparkContext().parallelize
                (Arrays.asList(model.getLdaModel().describeTopics(maxWords)));


        JavaRDD<TopicRow> rows = topics
                .repartition(context.getRecommendedPartitions())
                .zipWithIndex()
                .map(pair -> {
                    TopicRow topicRow = new TopicRow();
                    String topicUri = URIGenerator.compositionFromId(Resource.Type.DOMAIN, corpusId, Resource.Type
                            .TOPIC, String.valueOf(pair._2));
                    topicRow.setUri(topicUri);
                    topicRow.setId(pair._2);
                    topicRow.setDate(TimeUtils.asISO());
                    topicRow.setElements(Arrays.stream(pair._1._1).mapToObj(index -> broadcastVocabulary
                            .getValue()[index]).collect(Collectors.toList()));
                    topicRow.setScores(Arrays.stream(pair._1._2).boxed().collect(Collectors.toList()));
                    topicRow.setDescription(topicRow.getElements().stream().limit(10).collect(Collectors.joining(" ")));
                    return topicRow;
                });

        // Save in database
        LOG.info("saving topics from " + URIGenerator.fromId(Resource.Type.DOMAIN, corpusId) + " to database..");

        context.getSqlContext()
                .createDataFrame(rows, TopicRow.class)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", TopicsDao.TABLE, "keyspace", SessionManager.getKeyspaceFromId(corpusId)))
                .save()
        ;


        LOG.info("saved!");

    }

    public TopicModel train(Corpus corpus){

        String domainUri = URIGenerator.fromId(Resource.Type.DOMAIN, corpus.getId());

        String optimizerId = helper.getOptimizerCache().getOptimizer(domainUri);

        LOG.info("LDA Optimizer for '"+domainUri +"' is " + optimizerId);
        LDAOptimizer ldaOptimizer = helper.getLdaOptimizerFactory().by(optimizerId);

        // LDA parameter optimization
        LDAParameters ldaParameters = ldaOptimizer.getParametersFor(corpus);
        LOG.info("LDA parameters calculated by " + ldaOptimizer+ ": " + ldaParameters);

        // Update Counter
        helper.getCounterDao().increment(domainUri, Resource.Type.TOPIC.route(), Long.valueOf(ldaParameters.getK()));

        // LDA parametrization
        RDD<Tuple2<Object, Vector>> documents = corpus.getBagOfWords();
        LDA lda = new LDA()
                .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
                .setK(ldaParameters.getK())
                .setMaxIterations(ldaParameters.getIterations())
                .setDocConcentration(ldaParameters.getAlpha())
                .setTopicConcentration(ldaParameters.getBeta())
                ;

        LOG.info("Building a new LDA Model (iter="+ldaParameters.getIterations()+") "+ldaParameters+" from "+ corpus.getSize() + " " +
                corpus.getTypes());
        Instant startModel  = Instant.now();
        LDAModel ldaModel   = lda.run(documents);

        Instant endModel    = Instant.now();
        LOG.info("LDA Model created in: "       + ChronoUnit.MINUTES.between(startModel,endModel) + "min " + (ChronoUnit
                .SECONDS.between(startModel,endModel)%60) + "secs");


        LocalLDAModel localLDAModel = (LocalLDAModel) ldaModel;

        TopicModel topicModel = new TopicModel(corpus.getId(),localLDAModel, corpus.getCountVectorizerModel());
        return topicModel;
    }


    public void saveToFileSystem(TopicModel model, String id ){
        try {
            helper.getStorageHelper().create(helper.getStorageHelper().absolutePath(helper.getStorageHelper().path(id, "")));

            // Clean previous model
            String ldaPath = helper.getStorageHelper().path(id, "lda");
            helper.getStorageHelper().deleteIfExists(ldaPath);

            // Save the model
            String absoluteModelPath = helper.getStorageHelper().absolutePath(helper.getStorageHelper().path(id, "lda/model"));
            LOG.info("Saving (or updating) the lda model at: " + absoluteModelPath);
            model.getLdaModel().save(context.getSparkContext().sc(), absoluteModelPath);

            //Save the vocabulary
            String absoluteVocabPath = helper.getStorageHelper().absolutePath(helper.getStorageHelper().path(id, "lda/vocabulary"));
            LOG.info("Saving (or updating) the lda vocab at: " + absoluteVocabPath);
            model.getVocabModel().save(absoluteVocabPath);

            this.helper.getLdaBuilder().getCache().refresh(new ComputingKey(context, id));

        }catch (Exception e){
            if (e instanceof java.nio.file.FileAlreadyExistsException) {
                LOG.warn(e.getMessage());
            }else {
                LOG.error("Error saving model", e);
            }
        }

    }
}
