/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.api;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.functions.TupleToResourceShape;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.*;
import org.librairy.modeler.lda.services.ShortestPathService;
import org.librairy.modeler.lda.services.SimilarityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class FreeTextAPI {

    private static final Logger LOG = LoggerFactory.getLogger(FreeTextAPI.class);

    @Autowired
    SimilarityService similarityService;

    @Autowired
    ModelingHelper helper;


    public List<SimilarResource> getSimilarResourcesTo(Text text, String domainUri, Double minScore, Integer maxResults, List<String> types) throws InterruptedException, DataNotFound {

        final ComputingContext context = helper.getComputingHelper().newContext("lda.similarity."+ URIGenerator.retrieveId(domainUri));

        try{
            TopicModel topicModel = helper.getLdaBuilder().load(context, URIGenerator.retrieveId(domainUri));

            // Create a minimal corpus
            Corpus corpus = new Corpus(context, "from-inference", Arrays.asList(new Resource.Type[]{Resource.Type.ANY}), helper);
            corpus.loadTexts(Arrays.asList(new Text[]{text}));

            // Use vocabulary from existing model
            corpus.setCountVectorizerModel(topicModel.getVocabModel());

            // Documents
            LOG.info("Creating bow from text..");
            RDD<Tuple2<Object, Vector>> documents = corpus.getBagOfWords();


            // Topic Distribution
            LOG.info("getting topic distributions from bow..");
            JavaRDD<ResourceShape> topicDistribution = topicModel
                    .getLdaModel()
                    .topicDistributions(documents)
                    .toJavaRDD()
                    .map(new TupleToResourceShape(text.getId()))
                    .cache();

            corpus.clean();

            ResourceShape resourceShape = topicDistribution.first();

            final double[] referenceVector = resourceShape.getVector();

            LOG.info("topic distribution is: " + Doubles.asList(referenceVector));

            // get centroids from domain
            DataFrame centroids = readCentroids(context, domainUri, types).repartition(context.getRecommendedPartitions());//.cache();
            long numCentroids = centroids.count();
            LOG.info(numCentroids + " centroids loaded from domain: " + domainUri);
            final Double minCentroidScore  = minScore / 2.0;
//
//            List<Row> validCentroids = centroids.toJavaRDD().filter(r ->{
//                double[] centroidVector = Doubles.toArray(r.getList(2));
//                double score = JensenShannonSimilarity.apply(centroidVector, referenceVector);
//                System.out.println("Centroid Vector: " + Doubles.asList(centroidVector));
//                System.out.println("Reference Vector: " + Doubles.asList(referenceVector));
//                System.out.println("Score: " + score);
//                return score > minCentroidScore;
//            }).collect();

            List<Row> validCentroids = centroids.toJavaRDD().collect();

            if (validCentroids.isEmpty()){
                LOG.info("No centroids found close enough to free-text");
                return Collections.emptyList();
            }

            LOG.info("Calculating similarity value to existing documents in sectors [" + validCentroids.stream().map(r -> r.getString(0)).collect(Collectors.toList()) + "]");
            List<SimilarResource> similarResources = new ArrayList<>();
            for (Row centroidRow : validCentroids){

                String centroidId = centroidRow.getString(0);

                DataFrame resources = readResources(context, domainUri, centroidId, types);

                List<SimilarResource> simResources = resources.toJavaRDD()
                        .filter(res -> JensenShannonSimilarity.apply(Doubles.toArray(res.getList(2)), referenceVector) > minScore)
                        .map(res -> {
                            SimilarResource sr = new SimilarResource();
                            sr.setUri(res.getString(0));
                            sr.setWeight(JensenShannonSimilarity.apply(Doubles.toArray(res.getList(2)), referenceVector));
                            sr.setTime(TimeUtils.asISO());
                            return sr;
                        }).collect();
                LOG.info(simResources.size() + " similar-enough resources to free text found in sector: " + centroidId);
                similarResources.addAll(simResources);
            }

            return similarResources.stream().sorted((a, b) -> -a.getWeight().compareTo(b.getWeight())).limit(maxResults).collect(Collectors.toList());

        }finally {
            LOG.info("Closing spark context");
            helper.getComputingHelper().close(context);
        }

    }



    private DataFrame readCentroids(ComputingContext context, String domainUri, List<String> types){
        DataFrame centroids = context.getCassandraSQLContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
                                DataTypes.createStructField(ShapesDao.RESOURCE_TYPE, DataTypes.StringType, false),
                                DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
                        }))
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode", "DROPMALFORMED")
                .options(ImmutableMap.of("table", ShapesDao.CENTROIDS_TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .load()
                .repartition(context.getRecommendedPartitions())
                .cache()
                ;
        centroids.take(1);

        if (types.isEmpty()) return centroids.filter( ShapesDao.RESOURCE_TYPE + "= '"+ Resource.Type.ANY.name().toLowerCase()+"'");

        String filterExpression = types.stream().map(type -> ShapesDao.RESOURCE_TYPE + "= '" + type + "' ").collect(Collectors.joining("or "));

        return centroids.filter(filterExpression);
    }

    private DataFrame readResources(ComputingContext context, String domainUri, String clusterId, List<String> types){

        return similarityService.loadSubgraphFromFileSystem(context, URIGenerator.retrieveId(domainUri), "nodes", clusterId, Optional.empty(), types);

    }

}
