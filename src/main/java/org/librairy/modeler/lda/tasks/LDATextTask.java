/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.*;
import org.librairy.modeler.lda.functions.RowToResourceShape;
import org.librairy.modeler.lda.functions.TupleToResourceShape;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDATextTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDATextTask.class);

    public static final String ROUTING_KEY_ID = "lda.text.calculated";

    private final ModelingHelper helper;

    public LDATextTask(ModelingHelper modelingHelper) {
        this.helper = modelingHelper;
    }


    public List<SimilarResource> getSimilar(Text text, Integer topValues, String domainUri, List<Resource.Type>
            types){

        if (Strings.isNullOrEmpty(text.getContent())) return Collections.emptyList();


        TopicModel topicModel = helper.getLdaBuilder().load(URIGenerator.retrieveId
                (domainUri));


        // Create a minimal corpus
        Corpus corpus = new Corpus("from-inference", Arrays.asList(new Resource.Type[]{Resource.Type.ANY}), helper);
        corpus.loadTexts(Arrays.asList(new Text[]{text}));

        // Use vocabulary from existing model
        corpus.setCountVectorizerModel(topicModel.getVocabModel());

        // Documents
        RDD<Tuple2<Object, Vector>> documents = corpus.getBagOfWords().cache();

        // Topic Distribution
        JavaRDD<ResourceShape> topicDistribution = topicModel
                .getLdaModel()
                .topicDistributions(documents)
                .toJavaRDD()
                .map(new TupleToResourceShape(text.getId()))
                .cache();

        // Read vectors from domain

        DataFrame baseDF = helper.getCassandraHelper().getContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType,
                                        false),
                                DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType),
                                        false)
                        }))
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode", "DROPMALFORMED")
                .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .load();

        DataFrame shapesDF = baseDF;

        // Filter by types
        if (!types.isEmpty()){
            Column condition = org.apache.spark.sql.functions.col("uri").contains("/"+types.get(0).route()+"/");

            if (types.size() > 1){
                for (int i=1; i< types.size(); i++){
                    condition = condition.or(org.apache.spark.sql.functions.col("uri").contains("/"+types.get(i).route
                            ()+"/"));
                }
            }
            shapesDF = baseDF.filter(condition);
        }

        JavaRDD<ResourceShape> shapes = shapesDF
                .toJavaRDD()
                .map(new RowToResourceShape());


        List<SimilarResource> similarResources = topicDistribution
                .cartesian(shapes)
                .map(pair -> {
                    SimilarResource sr = new SimilarResource();
                    sr.setUri(pair._2.getUri());
                    sr.setWeight(JensenShannonSimilarity.apply(pair._1.getVector(), pair._2.getVector()));
                    return sr;
                })
                .takeOrdered(topValues);

        return similarResources;

    }

    @Override
    public void run() {

//        helper.getUnifiedExecutor().execute(() -> {
//            try{
//
//
//
//
//
//            }catch (Exception e){
//                if (e instanceof InterruptedException) LOG.warn("Execution canceled");
//                else LOG.error("Error on execution", e);
//            }
//        });

    }


}
