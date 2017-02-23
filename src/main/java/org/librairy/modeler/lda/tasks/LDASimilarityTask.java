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
            final ComputingContext context = helper.getComputingHelper().newContext("lda.similarity."+ URIGenerator.retrieveId(domainUri));
            final Integer partitions = context.getRecommendedPartitions();

            helper.getComputingHelper().execute(context, () -> {
                try{

                    //drop similarity tables
                    helper.getSimilaritiesDao().destroy(domainUri);
                    helper.getClusterDao().destroy(domainUri);


                    DataFrame shapesDF = context.getCassandraSQLContext()
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


                    final long vectorDim = topics;

                    JavaRDD<Tuple2<String,Vector>> vectors = shapesDF
                            .toJavaRDD()
                            .map(new RowToTupleVector())
                            .filter(el -> el._2.size() == vectorDim)
                            .cache();

                    vectors.take(1); // force cache

                    AtomicInteger counter = new AtomicInteger();
                    CircularFifoQueue centroidsQueue = new CircularFifoQueue(5);

                    similaritiesFromCentroids(context, vectors, 2000, 1.3, 20, 0.00001, counter, centroidsQueue, 0.5);
//                similaritiesFromCentroids(vectors, 8, 1, 20, 0.00001, counter, centroidsQueue);

                    // Save centroids to filesystem
                    saveCentroidsToFileSystem(context);

                    // Save similarities btw centroids to filesystem
                    saveCentroidSimilaritiesToFileSystem(context);

                    LOG.info("Operation completed!");
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

    private void similaritiesFromCentroids(ComputingContext context, JavaRDD<Tuple2<String,Vector>> documents, int maxSize, double
            ratio, int maxIterations, double epsilon, AtomicInteger counter, CircularFifoQueue<Centroid> centroidsQueue, Double minScore){

        long size = documents.count();

        if (size < maxSize){
            Centroid centroid = new Centroid();
            centroid.setId(1l);
            saveSimilaritiesBetween(context, documents, centroid, Collections.emptyList(), minScore);
            // Increment counter
            if (size>0){
                Long combinations = CombinatoricsUtils.stirlingS2(Long.valueOf(size).intValue(),Long.valueOf(size-1).intValue());
                helper.getCounterDao().increment(domainUri, Relation.Type.SIMILAR_TO_DOCUMENTS.route(), combinations);
            }
            return;
        }

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

        final Long maxDim = topics;

        // Get vector based on topic distributions
        RDD<Vector> points = documents.filter(el -> el._2.size() == maxDim).map(el -> el._2).rdd().cache();

//        List<Tuple2<String, Vector>> errors = documents.filter(el -> el._2.size() != 300).collect();
//
//        for (Tuple2<String, Vector> el : errors){
//            LOG.info("> " + el._1 + " : " + el._2.size());
//        }
//
//
//        RDD<Vector> points = documents.map(el -> el._2.toDense().compressed()).rdd().cache();
//
//        // Create a RowMatrix from JavaRDD<Vector>.
//        RowMatrix mat = new RowMatrix(points);
//
//        // Compute the top 2 principal components.
//        Matrix pc = mat.computePrincipalComponents(2);
//        RowMatrix projected = mat.multiply(pc);

        // Train k-means model
        KMeansModel model = kmeans.run(points);

        // Clusterize documents
        JavaPairRDD<Long, Tuple2<String, Vector>> indexedDocs = documents
                .zipWithIndex()
                .mapToPair(tuple -> new Tuple2<Long, Tuple2<String, Vector>>(tuple._2, tuple._1))
                .cache();
        indexedDocs.take(1);

        JavaPairRDD<Integer, Tuple2<String, Vector>> clusterizedDocs = model.predict(points).toJavaRDD()
                .zipWithIndex()
                .repartition(context.getRecommendedPartitions())
                .mapToPair(tuple -> new Tuple2<Long, Integer>(tuple._2, (Integer) tuple._1))
                .join(indexedDocs, context.getRecommendedPartitions())
                .mapToPair(el -> el._2)
                .cache();
        clusterizedDocs.take(1);

        Vector[] centroids = model.clusterCenters();

        for (int cid = 0; cid < centroids.length ; cid ++){

            Vector centroidVector = centroids[cid];

            final int centroidId = cid;

            // points in cluster
            double maxDistance = minDistance(centroidVector, centroids)/ratio;
            JavaRDD<Tuple2<String, Vector>> clusterPoints = clusterizedDocs
                    .filter(t1 -> (t1._1 == centroidId) || (Vectors.sqdist(t1._2._2, centroidVector) < maxDistance))
                    .map(t2 -> t2._2)
                    .cache();
            clusterPoints.take(1);

            int index = counter.incrementAndGet();

            Centroid centroid = new Centroid();
            centroid.setId(Long.valueOf(index));
            centroid.setVector(centroidVector);
            long numPoints = clusterPoints.count();

            if (numPoints < maxSize){
                saveSimilaritiesBetween(context, clusterPoints, centroid, centroidsQueue.stream().collect(Collectors.toList()), minScore);
                LOG.info("similarities in cluster " + index + " calculated with " + numPoints + " documents");
                centroidsQueue.add(centroid);
                // Increment counter
                Long combinations = CombinatoricsUtils.stirlingS2(Long.valueOf(numPoints).intValue(),Long.valueOf
                        (numPoints-1).intValue());
                helper.getCounterDao().increment(domainUri, Relation.Type.SIMILAR_TO_DOCUMENTS.route(), combinations);
            }else if (numPoints == size){
                saveSimilaritiesBetween(context, clusterPoints, centroid, centroidsQueue.stream().collect(Collectors.toList()), minScore);
                centroidsQueue.add(centroid);
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
                similaritiesFromCentroids(context, clusterPoints,maxSize, ratio, maxIterations, epsilon, counter, centroidsQueue, minScore);
            }
        }
    }

    private void saveCentroidsToFileSystem(ComputingContext context){
        DataFrame dataFrame = context.getCassandraSQLContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
                                DataTypes.createStructField(ShapesDao.RESOURCE_TYPE, DataTypes.StringType, false)
                        }))
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode", "DROPMALFORMED")
                .options(ImmutableMap.of("table", ShapesDao.CENTROIDS_TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .load()
                .repartition(context.getRecommendedPartitions())
                .cache();

        LOG.info("Saving centroids in filesystem ...");
        helper.getSimilarityService().saveCentroids(domainUri, dataFrame);
    }

    private void saveCentroidSimilaritiesToFileSystem(ComputingContext context){
        DataFrame dataFrame = context.getCassandraSQLContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType, false),
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType, false),
                                DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType, false),
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_1, DataTypes.StringType, false),
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_2, DataTypes.StringType, false)
                        }))
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode", "DROPMALFORMED")
                .options(ImmutableMap.of("table", SimilaritiesDao.CENTROIDS_TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .load()
                .repartition(context.getRecommendedPartitions())
                .cache();

        LOG.info("Saving centroid-similarities in filesystem ...");
        helper.getSimilarityService().saveCentroidSimilarities(domainUri, dataFrame);
    }




    private void saveSimilaritiesBetween(ComputingContext context, JavaRDD<Tuple2<String,Vector>> vectors, Centroid centroid, List<Centroid> neighbors, Double minScore){
        JavaRDD<SimilarityRow> rows = vectors
                .cartesian(vectors)
                .repartition(context.getRecommendedPartitions())
                .map(pair -> {
                    double score = JensenShannonSimilarity.apply(pair._1._2.toArray(), pair._2._2.toArray());
                    SimilarityRow row1 = new SimilarityRow();
                    row1.setDate(TimeUtils.asISO());
                    row1.setResource_uri_1(pair._1._1);
                    row1.setResource_type_1(URIGenerator.typeFrom(pair._1._1).key());
                    row1.setResource_uri_2(pair._2._1);
                    row1.setResource_type_2(URIGenerator.typeFrom(pair._2._1).key());
                    row1.setScore(score);
                    return row1;

                })
                .filter(r -> (r.getScore() >= minScore) && (!r.getResource_uri_1().equalsIgnoreCase(r.getResource_uri_2())))
                .cache();

        rows.take(1); // force cache

        LOG.info("calculating similarities btw documents in sector " + centroid.getId());
        context.getSqlContext()
                .createDataFrame(rows, SimilarityRow.class)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", SimilaritiesDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .mode(SaveMode.Append)
                .save();

        //TODO Save to filesystem in Append Mode

        //Save centroid
        ShapeRow centroidRow = new ShapeRow();
        centroidRow.setId(centroid.getId());
        centroidRow.setUri(String.valueOf(centroid.getId()));
        centroidRow.setType(centroid.getType());
        if (centroid.getVector() != null){
            double[] arrayVal = centroid.getVector().toArray();
            centroidRow.setVector(Doubles.asList(arrayVal));
        }
        centroidRow.setDate(TimeUtils.asISO());
        LOG.info("saving sector " + centroid.getId());
        helper.getShapesDao().saveCentroid(domainUri, centroidRow);


        //Save centroid similarities
        if (!neighbors.isEmpty()){
            JavaRDD<Centroid> centroidRDD = context.getSparkContext().parallelize(Arrays.asList(new
                    Centroid[]{centroid}));
            JavaRDD<Centroid> neighborsRDD = context.getSparkContext().parallelize(neighbors);
            JavaRDD<SimilarityRow> centroidRows = centroidRDD
                    .cartesian(neighborsRDD)
                    .flatMap(pair -> {
                        double score = JensenShannonSimilarity.apply(pair._1.getVector().toArray(), pair._2.getVector().toArray());
                        SimilarityRow row1 = new SimilarityRow();
                        row1.setDate(TimeUtils.asISO());
                        row1.setResource_uri_1(String.valueOf(pair._1.getId()));
                        row1.setResource_type_1(pair._1.getType());
                        row1.setResource_uri_2(String.valueOf(pair._2.getId()));
                        row1.setResource_type_2(pair._2.getType());
                        row1.setScore(score);

                        SimilarityRow row2 = new SimilarityRow();
                        row2.setDate(TimeUtils.asISO());
                        row2.setResource_uri_1(String.valueOf(pair._2.getId()));
                        row2.setResource_type_1(pair._2.getType());
                        row2.setResource_uri_2(String.valueOf(pair._1.getId()));
                        row2.setResource_type_2(pair._1.getType());
                        row2.setScore(score);

                        return Arrays.asList(new SimilarityRow[]{row1, row2});
                    });
            LOG.debug("saving centroid-similarities from sector " + centroid.getId() + " to: " + neighbors);
            context.getSqlContext()
                    .createDataFrame(centroidRows, SimilarityRow.class)
                    .write()
                    .format("org.apache.spark.sql.cassandra")
                    .options(ImmutableMap.of("table", SimilaritiesDao.CENTROIDS_TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                    .mode(SaveMode.Append)
                    .save();
        }

        //TODO Save cluster for each document
        JavaRDD<ClusterRow> clusterRows = vectors.map(tuple -> {
            ClusterRow clusterRow = new ClusterRow();
            clusterRow.setUri(tuple._1);
            clusterRow.setCluster(centroid.getId());
            return clusterRow;
        });
        LOG.info("saving cluster "+centroid.getId() + " of documents");
        context.getSqlContext()
                .createDataFrame(clusterRows, ClusterRow.class)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", ClusterDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .mode(SaveMode.Append)
                .save();

        //TODO Save subgraph

        // nodes
        StructType nodeDataType = DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
                        DataTypes.createStructField(ShapesDao.RESOURCE_TYPE, DataTypes.StringType, false)
                });

        JavaRDD<Row> nodeRows = vectors.map(tuple -> RowFactory.create(tuple._1, URIGenerator.typeFrom(tuple._1).name().toLowerCase()));
        DataFrame nodesFrame = context.getCassandraSQLContext().createDataFrame(nodeRows, nodeDataType);
        LOG.info("saving subgraph nodes from sector " + centroid.getId());
        helper.getSimilarityService().saveSubGraphToFileSystem(nodesFrame,URIGenerator.retrieveId(domainUri), "nodes", String.valueOf(centroid.getId()));



        // edges
        StructType edgeDataType = DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_1, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_2, DataTypes.StringType, false)
                });
        JavaRDD<Row> edgeRows = rows.map(sr -> RowFactory.create(sr.getResource_uri_1(), sr.getResource_uri_2(), sr.getScore(), sr.getResource_type_1(), sr.getResource_type_2()));
        DataFrame edgesFrame = context.getCassandraSQLContext().createDataFrame(edgeRows, edgeDataType);
        LOG.info("saving subgraph edges from sector " + centroid.getId());
        helper.getSimilarityService().saveSubGraphToFileSystem(edgesFrame,URIGenerator.retrieveId(domainUri), "edges", String.valueOf(centroid.getId()));
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
