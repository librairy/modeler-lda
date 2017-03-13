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
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.*;
import org.librairy.modeler.lda.functions.RowToResourceShape;
import org.librairy.modeler.lda.functions.RowToTupleVector;
import org.librairy.modeler.lda.functions.TupleToResourceShape;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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


    public List<SimilarResource> getSimilar(Text text, Integer topValues, String domainUri, List<Resource.Type> types){

        List<SimilarResource> similarResources = Collections.emptyList();

        if (Strings.isNullOrEmpty(text.getContent())) return similarResources;

        try{
            final ComputingContext context = helper.getComputingHelper().newContext("lda.text."+ URIGenerator.retrieveId(domainUri));
            try{
                TopicModel topicModel = helper.getLdaBuilder().load(context, URIGenerator.retrieveId(domainUri));

                // Create a minimal corpus
                Corpus corpus = new Corpus(context, "from-inference", Arrays.asList(new Resource.Type[]{Resource.Type.ANY}), helper);
                corpus.loadTexts(Arrays.asList(new Text[]{text}));

                // Use vocabulary from existing model
                corpus.setCountVectorizerModel(topicModel.getVocabModel());

                // Documents
                RDD<Tuple2<Object, Vector>> documents = corpus.getBagOfWords();


                LOG.info("Created bow from text");

                // Topic Distribution
                JavaRDD<ResourceShape> topicDistribution = topicModel
                        .getLdaModel()
                        .topicDistributions(documents)
                        .toJavaRDD()
                        .map(new TupleToResourceShape(text.getId()))
                        .cache();

                corpus.clean();

                LOG.info("Created topic-based vector from text");

                // Read vectors from domain
                DataFrame baseDF = context.getCassandraSQLContext()
                        .read()
                        .format("org.apache.spark.sql.cassandra")
                        .schema(DataTypes
                                .createStructType(new StructField[]{
                                        DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType,
                                                false),
                                        DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType),
                                                false),
                                        DataTypes.createStructField(ShapesDao.RESOURCE_TYPE, DataTypes.StringType,
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
                    Column condition = org.apache.spark.sql.functions.col(ShapesDao.RESOURCE_TYPE).equalTo(types.get(0).key());
                    shapesDF = baseDF.filter(condition);
                }

                LOG.info("Calculating similarity value to existing documents..");

                JavaRDD<ResourceShape> shapes = shapesDF
                        .repartition(context.getRecommendedPartitions())
                        .toJavaRDD()
                        .map(new RowToResourceShape())
                        .cache();

                shapes.take(1);

                similarResources = topicDistribution
                        .cartesian(shapes)
                        .map(pair -> {
                            SimilarResource sr = new SimilarResource();
                            sr.setUri(pair._2.getUri());
                            sr.setWeight(JensenShannonSimilarity.apply(pair._1.getVector(), pair._2.getVector()));
                            sr.setTime(TimeUtils.asISO());
                            return sr;
                        })
                        .takeOrdered(topValues);


                shapes.unpersist();

                topicDistribution.unpersist();

            }catch (Exception e){
                LOG.error("Unexpected error getting similar resources", e);
            }finally {
                helper.getComputingHelper().close(context);
            }
        } catch (InterruptedException e) {
            LOG.info("Execution interrupted.");
        }

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
