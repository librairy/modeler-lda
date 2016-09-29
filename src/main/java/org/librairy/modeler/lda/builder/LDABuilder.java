/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.builder;

import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.clustering.OnlineLDAOptimizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.librairy.computing.helper.SparkHelper;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.modeler.lda.optimizers.LDAOptimizer;
import org.librairy.modeler.lda.optimizers.LDAParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

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
    LDAOptimizer ldaOptimizer;

    @Value("#{environment['LIBRAIRY_LDA_MAX_ITERATIONS']?:${librairy.lda.maxiterations}}")
    Integer maxIterations;

    public TopicModel build(Corpus corpus){

        // Train
        TopicModel model = train(corpus);

        // Persist
        persist(model,corpus.getId());

        return model;

    }

    public TopicModel train(Corpus corpus){

        // LDA parameter optimization
        LDAParameters ldaParameters = ldaOptimizer.getParametersFor(corpus);
        LOG.info("LDA parameters calculated by " + ldaOptimizer+ ": " + ldaParameters);

        // LDA parametrization
        RDD<Tuple2<Object, Vector>> documents = corpus.getBagOfWords().cache();
        LDA lda = new LDA()
                .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
                .setK(ldaParameters.getK())
                .setMaxIterations(maxIterations)
                .setDocConcentration(ldaParameters.getAlpha())
                .setTopicConcentration(ldaParameters.getBeta())
                ;

        LOG.info("Building a new LDA Model (iter="+maxIterations+") "+ldaParameters+" from "+
                corpus.getSize() + " documents");
        Instant startModel  = Instant.now();
        LDAModel ldaModel   = lda.run(documents);
        Instant endModel    = Instant.now();
        LOG.info("LDA Model created in: "       + ChronoUnit.MINUTES.between(startModel,endModel) + "min " + (ChronoUnit
                .SECONDS.between(startModel,endModel)%60) + "secs");


        LocalLDAModel localLDAModel = (LocalLDAModel) ldaModel;

        TopicModel topicModel = new TopicModel(corpus.getId(),localLDAModel, corpus.getCountVectorizerModel());
        return topicModel;
    }


    public void persist(TopicModel model, String id ){
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

}
