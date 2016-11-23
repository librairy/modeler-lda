/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.builder;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.clustering.OnlineLDAOptimizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.librairy.computing.helper.SparkHelper;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.TopicRow;
import org.librairy.modeler.lda.dao.TopicsDao;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.modeler.lda.optimizers.LDAOptimizer;
import org.librairy.modeler.lda.optimizers.LDAParameters;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class LDABuilder {

    private static Logger LOG = LoggerFactory.getLogger(LDABuilder.class);

    @Autowired
    SparkHelper sparkHelper;

    @Autowired
    StorageHelper storageHelper;

    @Autowired
    ModelingHelper helper;

    @Autowired
    LDAOptimizer ldaOptimizer;

    @Value("#{environment['LIBRAIRY_LDA_MAX_ITERATIONS']?:${librairy.lda.maxiterations}}")
    Integer maxIterations;

    @Value("#{environment['LIBRAIRY_LDA_WORDS_PER_TOPIC']?:${librairy.lda.topic.words}}")
    Integer maxWords;

    private LoadingCache<String, TopicModel> cache;

    @PostConstruct
    public void setup(){
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, TopicModel>() {
                            public TopicModel load(String id) {
                                // Load the model
                                String modelPath = storageHelper.absolutePath(storageHelper.path(id,"lda/model"));
                                LOG.info("loading lda model from :" + modelPath);
                                LocalLDAModel localLDAModel = LocalLDAModel.load(sparkHelper.getContext().sc(), modelPath);

                                //Load the CountVectorizerModel
                                String vocabPath = storageHelper.absolutePath(storageHelper.path(id,"lda/vocabulary"));
                                LOG.info("loading lda vocabulary from :" + vocabPath);
                                CountVectorizerModel vocabModel = CountVectorizerModel.load(vocabPath);
                                return new TopicModel(id,localLDAModel, vocabModel);
                            }
                        });

    }

    public TopicModel build(Corpus corpus){

        // Train
        TopicModel model = train(corpus);

        // Persist in File System
        saveToFileSystem(model,corpus.getId());

        // Persist in Data Base
        saveToDataBase(model,corpus.getId());

        return model;

    }

    public void saveToDataBase(TopicModel model, String corpusId){


        Broadcast<String[]> broadcastVocabulary = sparkHelper.getContext().broadcast(model.getVocabModel().vocabulary
                ());


        JavaRDD<Tuple2<int[], double[]>> topics = sparkHelper.getContext().parallelize
                (Arrays.asList(model.getLdaModel().describeTopics(maxWords)));


        JavaRDD<TopicRow> rows = topics
                .repartition(helper.getPartitioner().estimatedFor(topics))
                .zipWithIndex()
                .map(pair -> {
                    TopicRow topicRow = new TopicRow();
                    topicRow.setUri(URIGenerator.fromContent(Resource.Type.TOPIC, "topic" + pair._2 + System.currentTimeMillis
                            ()));
                    topicRow.setId(pair._2);
                    topicRow.setDate(TimeUtils.asISO());
                    topicRow.setElements(Arrays.stream(pair._1._1).mapToObj(index -> broadcastVocabulary
                            .getValue()[index]).collect(Collectors.toList()));
                    topicRow.setScores(Arrays.asList(ArrayUtils.toObject(pair._1._2)));
                    topicRow.setDescription(topicRow.getElements().stream().limit(10).collect(Collectors.joining(" ")));
                    return topicRow;
                });

        // Save in database
        LOG.info("saving topics from " + URIGenerator.fromId(Resource.Type.DOMAIN, corpusId) + " to database..");
        CassandraJavaUtil.javaFunctions(rows)
                .writerBuilder(SessionManager.getKeyspaceFromId(corpusId), TopicsDao.TABLE, mapToRow(TopicRow.class))
                .saveToCassandra();
        LOG.info("saved!");

    }

    public TopicModel train(Corpus corpus){

        // LDA parameter optimization
        LDAParameters ldaParameters = ldaOptimizer.getParametersFor(corpus);
        LOG.info("LDA parameters calculated by " + ldaOptimizer+ ": " + ldaParameters);

        // LDA parametrization
        RDD<Tuple2<Object, Vector>> documents = corpus.getBagOfWords();
        LDA lda = new LDA()
                .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
                .setK(ldaParameters.getK())
                .setMaxIterations(maxIterations)
                .setDocConcentration(ldaParameters.getAlpha())
                .setTopicConcentration(ldaParameters.getBeta())
                ;

        LOG.info("Building a new LDA Model (iter="+maxIterations+") "+ldaParameters+" from "+ corpus.getSize() + " " +
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

            // Clean previous model
            String ldaPath = storageHelper.path(id, "lda");
            storageHelper.deleteIfExists(ldaPath);

            // Save the model
            String absoluteModelPath = storageHelper.absolutePath(storageHelper.path(id, "lda/model"));
            LOG.info("Saving (or updating) the lda model at: " + absoluteModelPath);
            model.getLdaModel().save(sparkHelper.getContext().sc(), absoluteModelPath);

            //Save the vocabulary
            String absoluteVocabPath = storageHelper.absolutePath(storageHelper.path(id, "lda/vocabulary"));
            LOG.info("Saving (or updating) the lda vocab at: " + absoluteVocabPath);
            model.getVocabModel().save(absoluteVocabPath);

        }catch (Exception e){
            if (e instanceof java.nio.file.FileAlreadyExistsException) {
                LOG.warn(e.getMessage());
            }else {
                LOG.error("Error saving model", e);
            }
        }

    }

    public TopicModel load(String id){
        try {
            return this.cache.get(id);
        } catch (ExecutionException e) {
            throw new RuntimeException("Error getting model and vocabulary from domain: " + id);
        }
    }

}
