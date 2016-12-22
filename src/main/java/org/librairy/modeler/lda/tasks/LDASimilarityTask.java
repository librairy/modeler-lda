/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.dao.SimilaritiesDao;
import org.librairy.modeler.lda.dao.SimilarityRow;
import org.librairy.modeler.lda.functions.RowToTupleVector;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDASimilarityTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDASimilarityTask.class);

    public static final String ROUTING_KEY_ID = "lda.similarities.created";

    private final ModelingHelper helper;

    private final String domainUri;

    public LDASimilarityTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        helper.getUnifiedExecutor().execute(() -> {
            try{

                int partitions = Runtime.getRuntime().availableProcessors() * 3;

                DataFrame shapesDF = helper.getCassandraHelper().getContext()
                        .read()
                        .format("org.apache.spark.sql.cassandra")
                        .schema(DataTypes
                                .createStructType(new StructField[]{
                                        DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType,
                                                false),
                                        DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType
                                                (DataTypes.DoubleType),
                                                false)
                                }))
                        .option("inferSchema", "false") // Automatically infer data types
                        .option("charset", "UTF-8")
//                        .option("spark.sql.autoBroadcastJoinThreshold","-1")
                        .option("mode", "DROPMALFORMED")
                        .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", SessionManager
                                .getKeyspaceFromUri
                                        (domainUri)))
                        .load()
                        .repartition(partitions)
                        .cache();

                shapesDF.take(1);


                JavaRDD<Tuple2<String,Vector>> vectors = shapesDF
                        .toJavaRDD()
                        .map(new RowToTupleVector())
                        .cache();

                vectors.take(1); // force cache

                AtomicInteger counter = new AtomicInteger();

                similaritiesFromCentroids(vectors, 2000, 1.3, 20, 0.00001, counter);

                LOG.info("Operation completed!");
                helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(ROUTING_KEY_ID));

            } catch (Exception e){
                // TODO Notify to event-bus when source has not been added
                LOG.error("Error scheduling a new topic model for Items from domain: " + domainUri, e);
            }
        });
        
    }

    private void similaritiesFromCentroids(JavaRDD<Tuple2<String,Vector>> documents, int maxSize, double
            ratio, int
            maxIterations, double epsilon, AtomicInteger counter){

        int partitions = Runtime.getRuntime().availableProcessors() * 3;

        long size = documents.count();

        int k = Double.valueOf(Math.ceil(Long.valueOf(size).doubleValue() / Integer.valueOf(maxSize).doubleValue())).intValue();

        if (k == 1){
            k = 2;
        }

        LOG.info("Searching " + k + " centroids for " + size + " points by[" +
                "maxSize=" + maxSize
                + "/ ratio=" + ratio+"]");

        KMeans kmeans = new KMeans()
                .setK(k)
                .setMaxIterations(maxIterations)
                .setEpsilon(epsilon)
                ;


        // Train k-means model
        RDD<Vector> points = documents.map(el -> el._2).rdd().cache();

        KMeansModel model = kmeans.run(points);

        // Clusterize documents
        JavaPairRDD<Long, Tuple2<String, Vector>> indexedDocs = documents
                .zipWithIndex()
                .mapToPair(tuple -> new Tuple2<Long, Tuple2<String, Vector>>(tuple._2, tuple._1))
                .cache();
        indexedDocs.take(1);

        JavaPairRDD<Integer, Tuple2<String, Vector>> clusterizedDocs = model.predict(points).toJavaRDD()
                .zipWithIndex()
                .mapToPair(tuple -> new Tuple2<Long, Integer>(tuple._2, (Integer) tuple._1))
                .join(indexedDocs, partitions)
                .mapToPair(el -> el._2)
                .cache();
        clusterizedDocs.take(1);

        Vector[] centroids = model.clusterCenters();

        for (int cid = 0; cid < centroids.length ; cid ++){

            Vector centroid = centroids[cid];

            final int centroidId = cid;

            // points in cluster
            double maxDistance = minDistance(centroid, centroids)/ratio;
            JavaRDD<Tuple2<String, Vector>> clusterPoints = clusterizedDocs
                    .filter(t1 -> (t1._1 == centroidId) || (Vectors.sqdist(t1._2._2, centroid) < maxDistance))
                    .map(t2 -> t2._2)
                    .cache();
            clusterPoints.take(1);

            int index = counter.incrementAndGet();

            long numPoints = clusterPoints.count();

            if (numPoints < maxSize){
                saveSimilaritiesBetween(clusterPoints);
                LOG.info("similarities in cluster " + index + " calculated with " + numPoints + " documents");
                // Increment counter
                Long combinations = CombinatoricsUtils.stirlingS2(Long.valueOf(numPoints).intValue(),Long.valueOf
                        (numPoints-1).intValue());
                helper.getCounterDao().increment(domainUri, Relation.Type.SIMILAR_TO_DOCUMENTS.route(), combinations);
            }else if (numPoints == size){
                saveSimilaritiesBetween(clusterPoints);
                LOG.warn("similarities in cluster " + index + " calculated with " + numPoints + " documents (exceeds the" +
                        " " +
                        "max points threshold!)");
                // Increment counter
                Long combinations = CombinatoricsUtils.stirlingS2(Long.valueOf(numPoints).intValue(),Long.valueOf
                        (numPoints-1).intValue());
                helper.getCounterDao().increment(domainUri, Relation.Type.SIMILAR_TO_DOCUMENTS.route(), combinations);
                break;
            } else{
                counter.decrementAndGet();
                similaritiesFromCentroids(clusterPoints,maxSize, ratio, maxIterations, epsilon, counter);
            }

        }
    }

    private void saveSimilaritiesBetween(JavaRDD<Tuple2<String,Vector>> vectors){
        JavaRDD<SimilarityRow> rows = vectors
                .cartesian(vectors)
                .filter(pair -> pair._1._1.hashCode() < pair._2._1.hashCode())
                .flatMap(pair -> {
                    double score = JensenShannonSimilarity.apply(pair._1._2.toArray(), pair._2._2.toArray());
                    SimilarityRow row1 = new SimilarityRow();
                    row1.setDate(TimeUtils.asISO());
                    row1.setResource_uri_1(pair._1._1);
                    row1.setResource_type_1(URIGenerator.typeFrom(pair._1._1).key());
                    row1.setResource_uri_2(pair._2._1);
                    row1.setResource_type_2(URIGenerator.typeFrom(pair._2._1).key());
                    row1.setScore(score);

                    SimilarityRow row2 = new SimilarityRow();
                    row2.setDate(TimeUtils.asISO());
                    row2.setResource_uri_1(pair._2._1);
                    row2.setResource_type_1(URIGenerator.typeFrom(pair._2._1).key());
                    row2.setResource_uri_2(pair._1._1);
                    row2.setResource_type_2(URIGenerator.typeFrom(pair._1._1).key());
                    row2.setScore(score);

                    return Arrays.asList(new SimilarityRow[]{row1,row2});

                });

        //TODO Save to filesystem in Append Mode

        CassandraJavaUtil.javaFunctions(rows)
                .writerBuilder(SessionManager.getKeyspaceFromUri(domainUri), SimilaritiesDao.TABLE, mapToRow(SimilarityRow.class))
                .saveToCassandra();
    }

    private double minDistance(Vector point, Vector[] points){
        double minValue = Double.MAX_VALUE;

        for (Vector reference: points){

            double distance = Vectors.sqdist(point, reference);

            if ((distance < minValue) && (!reference.equals(point))){
                minValue = distance;
            }

        }
        return minValue;
    }


}
