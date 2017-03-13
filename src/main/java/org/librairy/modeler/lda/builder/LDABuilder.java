/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.builder;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.Getter;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.librairy.boot.storage.dao.CounterDao;
import org.librairy.boot.storage.dao.ParametersDao;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.computing.helper.ComputingHelper;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.ComputingKey;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.modeler.lda.tasks.LDABuildTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class LDABuilder {

    private static Logger LOG = LoggerFactory.getLogger(LDABuilder.class);

    @Autowired
    StorageHelper storageHelper;

    @Autowired
    ModelingHelper helper;


    @Autowired
    ParametersDao parametersDao;

    @Autowired
    CounterDao counterDao;

    @Value("#{environment['LIBRAIRY_LDA_WORDS_PER_TOPIC']?:${librairy.lda.topic.words}}")
    Integer maxWords;

    @Getter
    public LoadingCache<ComputingKey, TopicModel> cache;

    @PostConstruct
    public void setup(){
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<ComputingKey, TopicModel>() {
                            public TopicModel load(ComputingKey key) {
                                // Load the model
                                String modelPath = storageHelper.absolutePath(storageHelper.path(key.getId(),"lda/model"));
                                LOG.info("loading lda model from :" + modelPath);
                                LocalLDAModel localLDAModel = LocalLDAModel.load(key.getContext().getSparkContext().sc(), modelPath);

                                //Load the CountVectorizerModel
                                String vocabPath = storageHelper.absolutePath(storageHelper.path(key.getId(),"lda/vocabulary"));
                                LOG.info("loading lda vocabulary from :" + vocabPath);
                                CountVectorizerModel vocabModel = CountVectorizerModel.load(vocabPath);
                                return new TopicModel(key.getId(),localLDAModel, vocabModel);
                            }
                        });

    }

    public TopicModel build(ComputingContext context, Corpus corpus){

        LDABuildTask task = new LDABuildTask(context, helper);

        TopicModel model =  task.create(corpus, maxWords);

        return model;

    }

    public TopicModel load(ComputingContext context, String id){
        try {
            ComputingKey key = new ComputingKey(context, id);
            return this.cache.get(key);
        } catch (ExecutionException e) {
            throw new RuntimeException("Error getting model and vocabulary from domain: " + id);
        }
    }

}
