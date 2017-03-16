/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.*;
import org.librairy.modeler.lda.functions.RowToTupleVector;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Centroid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

    private final Long topics;

    public LDASimilarityTask(String domainUri, ModelingHelper modelingHelper) throws DataNotFound {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
        this.topics = helper.getDomainsDao().count(domainUri, org.librairy.boot.model.domain.resources.Resource.Type.TOPIC.route());
    }


    @Override
    public void run() {

        try{
            helper.getCounterDao().reset(domainUri, Relation.Type.SIMILAR_TO_DOCUMENTS.route());

            final ComputingContext context = helper.getComputingHelper().newContext("lda.similarity."+ URIGenerator.retrieveId(domainUri));
            final Integer partitions = context.getRecommendedPartitions();

            helper.getComputingHelper().execute(context, () -> {
                try{

                    //drop similarity tables
                    helper.getSimilaritiesDao().destroy(domainUri);
                    helper.getClusterDao().destroy(domainUri);
                    helper.getShapesDao().destroyCentroids(domainUri);

                    final long vectorDim = topics;
                    JavaRDD<Tuple2<String,Vector>> vectors = context.getCassandraSQLContext()
                            .read()
                            .format("org.apache.spark.sql.cassandra")
                            .schema(DataTypes
                                    .createStructType(new StructField[]{
                                            DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
                                            DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
                                    }))
                            .option("inferSchema", "false") // Automatically infer data types
                            .option("charset", "UTF-8")
//                        .option("spark.sql.autoBroadcastJoinThreshold","-1")
                            .option("mode", "DROPMALFORMED")
                            .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                            .load()
                            .repartition(partitions)
                            .toJavaRDD()
                            .map(new RowToTupleVector())
                            .filter(el -> el._2.size() == vectorDim)
                            .cache();

                    vectors.take(1); // force cache

                    AtomicInteger counter = new AtomicInteger();

                    Integer maxPointsPerCluster = 2000;
                    Integer maxIterations = 20;
                    Double epsilon = 0.00001;
                    similaritiesFromCentroids(context, vectors, maxPointsPerCluster, maxIterations, epsilon, counter);

                    vectors.unpersist();

                    // Save centroids to filesystem
                    saveCentroidsToFileSystem(context);

                    LOG.info("Similarities calculated!");

                    helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(ROUTING_KEY_ID));

                } catch (Exception e){
                    // TODO Notify to event-bus when source has not been added
                    LOG.error("Error calculating similarities in domain: " + domainUri, e);
                }
            });
        } catch (InterruptedException e) {
            LOG.info("Execution interrupted.");
        }


    }

    private void similaritiesFromCentroids(ComputingContext context, JavaRDD<Tuple2<String,Vector>> documents, int maxSize, int maxIterations, double epsilon, AtomicInteger counter){

        long size = documents.count();

        if (size < maxSize){
            if (size == 0) return;
            Centroid centroid = new Centroid();
            centroid.setId(1l);
            saveSimilaritiesBetween(context, documents, centroid);
            // Increment counter
            Long combinations = CombinatoricsUtils.stirlingS2(Long.valueOf(size).intValue(),Long.valueOf(size-1).intValue());
            helper.getCounterDao().increment(domainUri, Relation.Type.SIMILAR_TO_DOCUMENTS.route(), combinations);
            return;
        }

        int k = Double.valueOf(Math.ceil(Long.valueOf(size).doubleValue() / Integer.valueOf(maxSize).doubleValue())).intValue();

        if (k == 1){
            k = 2;
        }

        LOG.info("Searching " + k + " centroids for " + size + " points by[" + "maxSize=" + maxSize +"]");

        KMeans kmeans = new KMeans()
                .setK(k)
                .setMaxIterations(maxIterations)
                .setEpsilon(epsilon)
                ;

        // Get vector based on topic distributions
        RDD<Vector> points = documents.map(el -> el._2).rdd().cache();
        points.take(1);

        // Train k-means model
        KMeansModel model = kmeans.run(points);

        // Clusterize documents
        JavaPairRDD<Long, Tuple2<String, Vector>> indexedDocs = documents
                .zipWithIndex()
                .mapToPair(tuple -> new Tuple2<Long, Tuple2<String, Vector>>(tuple._2, tuple._1));

        JavaPairRDD<Integer, Tuple2<String, Vector>> clusterizedDocs = model.predict(points)
                .toJavaRDD()
                .zipWithIndex()
                .repartition(context.getRecommendedPartitions())
                .mapToPair(tuple -> new Tuple2<Long, Integer>(tuple._2, (Integer) tuple._1))
                .join(indexedDocs, context.getRecommendedPartitions())
                .mapToPair(el -> el._2)
                .cache();
        clusterizedDocs.take(1);

        points.unpersist(false);

        Vector[] centroids = model.clusterCenters();

        for (int cid = 0; cid < centroids.length ; cid ++){

            Vector centroidVector = centroids[cid];

            // points in cluster
            final int centroidId = cid;
            JavaRDD<Tuple2<String, Vector>> clusterPoints = clusterizedDocs
                    .filter(t1 -> (t1._1 == centroidId))
                    .map(t2 -> t2._2)
                    .cache();
            long numPoints = clusterPoints.count();

            int index = counter.incrementAndGet();

            Centroid centroid = new Centroid();
            centroid.setId(Long.valueOf(index));
            centroid.setVector(centroidVector);


            if (numPoints <= maxSize){
                saveSimilaritiesBetween(context, clusterPoints, centroid);
                LOG.info("similarities in cluster " + index + " calculated with " + numPoints + " documents");
                // Increment counter
                Long combinations = CombinatoricsUtils.stirlingS2(Long.valueOf(numPoints).intValue(),Long.valueOf(numPoints-1).intValue());
                helper.getCounterDao().increment(domainUri, Relation.Type.SIMILAR_TO_DOCUMENTS.route(), combinations);

                clusterPoints.unpersist();

            } else{
                counter.decrementAndGet();
                similaritiesFromCentroids(context, clusterPoints,maxSize, maxIterations, epsilon, counter);
            }

        }

        clusterizedDocs.unpersist();

        documents.unpersist();

    }

    private void saveCentroidsToFileSystem(ComputingContext context){
        DataFrame dataFrame = context.getCassandraSQLContext()
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
        dataFrame.take(1);

        LOG.info("Saving centroids in filesystem ...");
        helper.getSimilarityService().saveCentroids(domainUri, dataFrame.select(ShapesDao.RESOURCE_URI, ShapesDao.RESOURCE_TYPE));

        LOG.info("Saving centroid-similarities in filesystem ...");

        JavaRDD<SimilarityRow> similarities = dataFrame.toJavaRDD().cartesian(dataFrame.toJavaRDD())
                .filter(r -> !r._1.getString(0).equals(r._2.getString(0)))
                .filter(r -> r._1.getString(1).equals(r._2.getString(1)))
                .map(t -> {
                            SimilarityRow row = new SimilarityRow();
                            row.setResource_uri_1(t._1.getString(0));
                            row.setResource_uri_2(t._2.getString(0));
                            row.setScore(JensenShannonSimilarity.apply(Vectors.dense(Doubles.toArray(t._1.getList(2))).toArray(), Vectors.dense(Doubles.toArray(t._2.getList(2))).toArray()));
                            row.setResource_type_1(t._1.getString(1));
                            row.setResource_type_2(t._2.getString(1));
                            row.setDate(TimeUtils.asISO());
                            return row;
                        }
                )
                .repartition(context.getRecommendedPartitions())
                .cache()
                ;
        similarities.take(1);

        dataFrame.unpersist();

        DataFrame similaritiesDF = context.getSqlContext().createDataFrame(similarities, SimilarityRow.class);

        similaritiesDF
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", SimilaritiesDao.CENTROIDS_TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .mode(SaveMode.Overwrite)
                .save();

        StructType simDataType = DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_1, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_2, DataTypes.StringType, false)
                });


        DataFrame similaritiesRowDF = context.getSqlContext().createDataFrame(similarities.map( sr -> RowFactory.create(sr.getResource_uri_1(), sr.getResource_uri_2(), sr.getScore(), sr.getResource_type_1(), sr.getResource_type_2())), simDataType);

        helper.getSimilarityService().saveCentroidSimilarities(domainUri, similaritiesRowDF);

        similarities.unpersist();

        LOG.info("Centroids saved!!");

    }


    private void saveSimilaritiesBetween(ComputingContext context, JavaRDD<Tuple2<String,Vector>> vectors, Centroid centroid){
        JavaRDD<SimilarityRow> simRows = vectors
                .cartesian(vectors)
                .repartition(context.getRecommendedPartitions())
                .filter( p -> !p._1._1.equals(p._2._1))
                .map(pair -> {
                    SimilarityRow row1 = new SimilarityRow();
                    row1.setResource_uri_1(pair._1._1);
                    row1.setResource_uri_2(pair._2._1);
                    row1.setScore(JensenShannonSimilarity.apply(pair._1._2.toArray(), pair._2._2.toArray()));
                    row1.setResource_type_1(URIGenerator.typeFrom(pair._1._1).key());
                    row1.setResource_type_2(URIGenerator.typeFrom(pair._2._1).key());
                    row1.setDate(TimeUtils.asISO());
                    return row1;

                })
                .cache()
                ;
        simRows.take(1);

        LOG.info("calculating similarities btw documents in sector " + centroid.getId());
        context.getSqlContext().createDataFrame(simRows, SimilarityRow.class)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", SimilaritiesDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .mode(SaveMode.Append)
                .save();

//        // edges
        StructType edgeDataType = DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_1, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_2, DataTypes.StringType, false)
                });
//        JavaRDD<Row> edgeRows = rows.map(sr -> RowFactory.create(sr.getResource_uri_1(), sr.getResource_uri_2(), sr.getScore(), sr.getResource_type_1(), sr.getResource_type_2()));
//        DataFrame edgesFrame = context.getCassandraSQLContext()
//                .createDataFrame(edgeRows, edgeDataType);


        LOG.info("saving subgraph edges from sector " + centroid.getId());
        JavaRDD<Row> rows = simRows.map(sr -> RowFactory.create(sr.getResource_uri_1(), sr.getResource_uri_2(), sr.getScore(), sr.getResource_type_1(), sr.getResource_type_2()));
        helper.getSimilarityService().saveSubGraphToFileSystem(context.getSqlContext().createDataFrame(rows, edgeDataType),URIGenerator.retrieveId(domainUri), "edges", String.valueOf(centroid.getId()));

        simRows.unpersist();

        //Save centroid
        ShapeRow centroidRow = new ShapeRow();
        centroidRow.setId(centroid.getId());
        centroidRow.setUri(String.valueOf(centroid.getId()));
        centroidRow.setType(Resource.Type.ANY.name().toLowerCase());
        centroidRow.setVector((centroid.getVector() != null)? Doubles.asList(centroid.getVector().toArray()) : calculateCentroid(vectors.map(t -> t._2).rdd()));
        centroidRow.setDate(TimeUtils.asISO());
        LOG.info("saving sector " + centroid.getId());
        helper.getShapesDao().saveCentroid(domainUri, centroidRow);

        saveCentroidByType(vectors, centroid.getId(), Resource.Type.ITEM);
        saveCentroidByType(vectors, centroid.getId(), Resource.Type.PART);
        saveCentroidByType(vectors, centroid.getId(), Resource.Type.DOMAIN);

        JavaRDD<ClusterRow> clusterRows = vectors.map(tuple -> {
            ClusterRow clusterRow = new ClusterRow();
            clusterRow.setUri(tuple._1);
            clusterRow.setCluster(centroid.getId());
            return clusterRow;
        });
        LOG.info("saving documents from sectot "+centroid.getId());
        context.getSqlContext()
                .createDataFrame(clusterRows, ClusterRow.class)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", ClusterDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .mode(SaveMode.Append)
                .save();


        // nodes
        StructType nodeDataType = DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
                        DataTypes.createStructField(ShapesDao.RESOURCE_TYPE, DataTypes.StringType, false),
                        DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
                });

        JavaRDD<Row> nodeRows = vectors.map(tuple -> RowFactory.create(tuple._1, URIGenerator.typeFrom(tuple._1).name().toLowerCase(), tuple._2.toArray()));
        DataFrame nodesFrame = context.getSqlContext().createDataFrame(nodeRows, nodeDataType);

        LOG.info("saving subgraph nodes from sector " + centroid.getId());
        helper.getSimilarityService().saveSubGraphToFileSystem(nodesFrame,URIGenerator.retrieveId(domainUri), "nodes", String.valueOf(centroid.getId()));

    }

    private void saveCentroidByType(JavaRDD<Tuple2<String,Vector>> vectors, Long id , Resource.Type type){

        RDD<Vector> resources = vectors.filter(t -> t._1.contains("/"+type.route()+"/")).map(t -> t._2).rdd();

        if (resources.count() == 0) return;

        ShapeRow centroidItemRow = new ShapeRow();
        long index = ((type.ordinal()+1) * 10000) + id;
        centroidItemRow.setId(index);
        centroidItemRow.setUri(String.valueOf(id));
        centroidItemRow.setType(type.key());
        centroidItemRow.setVector(calculateCentroid(resources));
        centroidItemRow.setDate(TimeUtils.asISO());
        LOG.info("saving ("+type.key()+") sector " + centroidItemRow.getId());
        helper.getShapesDao().saveCentroid(domainUri, centroidItemRow);

        resources.unpersist(false);
    }


    private List<Double> calculateCentroid(RDD<Vector> points){
        points.cache();
        points.take(1);

        // KMeans
//        KMeans kmeans = new KMeans().setK(1).setMaxIterations(20).setEpsilon(0.001);//0.0001
//        KMeansModel model = kmeans.run(points);


        // Bisecting KMeans
        BisectingKMeans bkm = new BisectingKMeans().setK(1).setMaxIterations(20);
        BisectingKMeansModel model = bkm.run(points);


        List<Double> centroid = Doubles.asList(model.clusterCenters()[0].toArray());
        points.unpersist(false);
        return centroid;

    }

//    private double minDistance(Vector point, Vector[] points){
//        double minValue = Double.MAX_VALUE;
//
//        for (Vector reference: points){
//
//            double distance = Vectors.sqdist(point, reference);
//
//            if ((distance < minValue) && (!reference.equals(point))){
//                minValue = distance;
//            }
//
//        }
//        return minValue;
//    }


}
