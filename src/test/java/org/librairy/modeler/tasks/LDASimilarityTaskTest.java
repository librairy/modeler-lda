/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.tasks;

import com.google.common.collect.ImmutableMap;
import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.Application;
import org.librairy.modeler.lda.builder.WorkspaceBuilder;
import org.librairy.modeler.lda.dao.ClusterDao;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.dao.SimilaritiesDao;
import org.librairy.modeler.lda.dao.SimilarityRow;
import org.librairy.modeler.lda.functions.RowToTupleVector;
import org.librairy.modeler.lda.functions.RowToVector;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Centroid;
import org.librairy.modeler.lda.tasks.LDADistributionsTask;
import org.librairy.modeler.lda.tasks.LDASimilarityTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import scala.Tuple2;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Created on 27/06/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@TestPropertySource({"classpath:boot.properties","classpath:computing.properties", "classpath:application.properties"})
public class LDASimilarityTaskTest {


    private static final Logger LOG = LoggerFactory.getLogger(LDASimilarityTaskTest.class);

    @Autowired
    ModelingHelper helper;

    @Autowired
    WorkspaceBuilder workspaceBuilder;

    @Test
    public void execute() throws InterruptedException, DataNotFound {
        String domainUri = "http://librairy.org/domains/blueBottle";

        LDASimilarityTask task = new LDASimilarityTask(domainUri, helper);

        task.run();

        LOG.info("Sleeping");
        Thread.currentThread().sleep(Integer.MAX_VALUE);

    }

    @Test
    public void publish(){

        String domainUri = "http://librairy.org/domains/default";

        helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(LDADistributionsTask.ROUTING_KEY_ID));
    }


    @Test
    public void dimsum() throws InterruptedException {

        String domainUri = "http://librairy.org/domains/default";

        final ComputingContext context = helper.getComputingHelper().newContext("test.dimsum");

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
                .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                .load()
                .repartition(32)
                .persist(helper.getCacheModeHelper().getLevel());


        shapesDF.take(1);

        RDD<Vector> vectors = shapesDF
                .toJavaRDD()
                .map(new RowToVector())
                .rdd()
                .persist(helper.getCacheModeHelper().getLevel());

        vectors.take(1); // force cache
////
        long size = vectors.count();
        LOG.info("" + size + " elements to be analyzed");

        RowMatrix rowMatrix = new RowMatrix(vectors);
        LOG.info("" + rowMatrix.numRows() + " num rows");
        LOG.info("" + rowMatrix.numCols() + " num cols");


        LOG.info("Computing similar columns with estimation using brute force");
        CoordinateMatrix simsPerfect = rowMatrix.columnSimilarities();
        LOG.info("" + simsPerfect.numRows() + " num total rows");
        LOG.info("" + simsPerfect.numCols() + " num total cols");
        LOG.info("" + simsPerfect.entries().count() + " num total");

        LOG.info("Computing similar columns with estimation using DIMSUM");
        CoordinateMatrix simsEstimate = rowMatrix.columnSimilarities(0.5);
        LOG.info("" + simsEstimate.numRows() + " num estimated rows");
        LOG.info("" + simsEstimate.numCols() + " num estimated cols");
        LOG.info("" + simsEstimate.entries().count() + " num total");

        List<MatrixEntry> sample = simsEstimate
                .entries()
                .toJavaRDD()
                .take(10);




        sample.forEach(el -> {
            LOG.info("Entry: " + el);


        });


        helper.getComputingHelper().close(context);

    }

    @Test
    public void simpleDimsum() throws InterruptedException {

        List<Vector> vectorList = new ArrayList<>();

        final ComputingContext context = helper.getComputingHelper().newContext("test.simplesum");

        Random random = new Random();

        for (int i =0; i<76; i++){
            vectorList.add(Vectors.dense(IntStream.range(0,5000).mapToDouble(id -> random.nextDouble()).toArray()));
        }

        RDD<Vector> vectors = context.getSparkContext().parallelize(vectorList,128)
//                .repartition(32)
                .rdd()
                .persist(helper.getCacheModeHelper().getLevel());

        vectors.take(1); // force cache
////
        long size = vectors.count();
        LOG.info("" + size + " elements to be analyzed");

        RowMatrix rowMatrix = new RowMatrix(vectors);
        LOG.info("" + rowMatrix.numRows() + " num rows");
        LOG.info("" + rowMatrix.numCols() + " num cols");


//        LOG.info("Computing similar columns using brute force");
//        CoordinateMatrix simsPerfect = rowMatrix.columnSimilarities();
//        LOG.info("" + simsPerfect.numRows() + " num total rows");
//        LOG.info("" + simsPerfect.numCols() + " num total cols");
//        LOG.info("" + simsPerfect.entries().count() + " num total");

        LOG.info("Computing similar columns with estimation using DIMSUM");
        CoordinateMatrix simsEstimate = rowMatrix.columnSimilarities(0.5);
        LOG.info("" + simsEstimate.numRows() + " num estimated rows");
        LOG.info("" + simsEstimate.numCols() + " num estimated cols");
        LOG.info("" + simsEstimate.entries().count() + " num total");

        List<MatrixEntry> sample = simsEstimate
                .entries()
                .toJavaRDD()
                .take(100);




        sample.forEach(el -> {
            LOG.info("Entry: " + el);


        });

        helper.getComputingHelper().close(context);
    }


    @Test
    public void recursiveKMeans() throws InterruptedException {
        String domainUri = "http://librairy.org/domains/default";

        final ComputingContext context = helper.getComputingHelper().newContext("test.recursiveKmeans");

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
                .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                .load()
                .repartition(32)
                .persist(helper.getCacheModeHelper().getLevel());


        shapesDF.take(1);

        JavaRDD<Vector> vectors = shapesDF
                .toJavaRDD()
                .map(new RowToVector())
//                .rdd()
                .persist(helper.getCacheModeHelper().getLevel());

        vectors.take(1); // force cache

        List<Tuple2<Vector, Double>> centroids = calculateCentroids(vectors, 2000, 1.3);


        LOG.info("Operation completed!");

        LOG.info("Discovered " + centroids.size() + " centroids");

        int counter = 0;

        JavaRDD<Tuple2<String,Vector>> docs = shapesDF
                .toJavaRDD()
                .map(new RowToTupleVector())
//                .rdd()
                .persist(helper.getCacheModeHelper().getLevel());

        for(Tuple2<Vector, Double> centroid : centroids){
                calculateSimilarities(context, docs, centroid, domainUri);
        }

        helper.getComputingHelper().close(context);

    }

    private List<Tuple2<Vector,Double>> calculateCentroids(JavaRDD<Vector> points, int maxSize, double ratio){

        List<Tuple2<Vector,Double>> result = new ArrayList<>();

        long size = points.count();

        int k = Double.valueOf(Math.ceil(Long.valueOf(size).doubleValue() / Integer.valueOf(maxSize).doubleValue())).intValue();

        if (k == 1){
            k = 2;
        }

        LOG.info("Generating " + k + " centroids for " + size + " points by[" +
                "maxSize=" + maxSize
                + "/ ratio=" + ratio+"]");
        KMeans kmeans = new KMeans()
                .setK(k)
                .setMaxIterations(20)
                .setEpsilon(0.00001)
                ;

        KMeansModel model = kmeans.run(points.rdd());

        Vector[] centroids = model.clusterCenters();

        for (Vector centroid : centroids){

            double maxDistance = minDistance(centroid, centroids)/ratio;

            JavaRDD<Vector> cluster = points
                    .filter(el -> (Vectors.sqdist(el, centroid) < maxDistance))
                    .persist(helper.getCacheModeHelper().getLevel());

            cluster.take(1);

            long numPoints = cluster.count();

            if (numPoints < maxSize){
                LOG.info("Centroid discovered containing " + numPoints + " points");
                result.add(new Tuple2<>(centroid,maxDistance));
            }else if (numPoints == size){
                LOG.warn("Centroid discovered containing " + numPoints + " points");
                result.add(new Tuple2<>(centroid,maxDistance));
                return result;
            } else{
                result.addAll(calculateCentroids(cluster,maxSize, ratio));
            }

        }


        return result;
    }
//
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

    private void calculateSimilarities(ComputingContext context, JavaRDD<Tuple2<String,Vector>> points, Tuple2<Vector,Double> centroid, String
                                       domainUri){
        JavaRDD<Tuple2<String,Vector>> cluster = points
                .filter(el -> (Vectors.sqdist(el._2, centroid._1) < centroid._2))
                .persist(helper.getCacheModeHelper().getLevel());

        cluster.take(1);

        long partialSize = cluster.count();

        LOG.info("Similarities based on [centroid: " + centroid._1.hashCode() + "/ maxDistance: " + centroid._2 + "]: "
                + partialSize + " documents");

//

//        JavaRDD<Row> rows = cluster
//                .cartesian(cluster)
//                .filter(pair -> pair._1._1.hashCode() < pair._2._1.hashCode())
//                .map(pair -> RowFactory.create(
//                        pair._1._1,
//                        URIGenerator.typeFrom(pair._1._1).key(),
//                        pair._2._1,
//                        URIGenerator.typeFrom(pair._2._1).key(),
//                        TimeUtils.asISO(),
//                        JensenShannonSimilarity.apply(pair._1._2.toArray(), pair._2._2.toArray())
//                        ));
//
//
//        // Define a schema
//        StructType schema = DataTypes
//                .createStructType(new StructField[] {
//                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType, false),
//                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_1, DataTypes.StringType, false),
//                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType, false),
//                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_2, DataTypes.StringType, false),
//                        DataTypes.createStructField(SimilaritiesDao.DATE, DataTypes.StringType, false),
//                        DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType, false)
//                });
//
//        sqlHelper.getContext()
//                .createDataFrame(rows, schema)
//                .write()
//                .format("org.apache.spark.sql.cassandra")
//                .options(ImmutableMap.of("table", SimilaritiesDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
//                .save()
//        ;
//        LOG.info("saved!");


        JavaRDD<SimilarityRow> rows = cluster
                .cartesian(cluster)
                .filter(pair -> pair._1._1.hashCode() < pair._2._1.hashCode())
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
                });

        context.getSqlContext()
                .createDataFrame(rows, SimilarityRow.class)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", SimilaritiesDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                .mode(SaveMode.Overwrite)
                .save();
        LOG.info("similarities saved!");

    }


    @Test
    public void updateSimCentroids() throws InterruptedException {

        String domainUri = "http://librairy.org/domains/141fc5bbcf0212ec9bee5ef66c6096ab";

        final ComputingContext context = helper.getComputingHelper().newContext("lda.similarity."+ URIGenerator.retrieveId(domainUri));

        helper.getComputingHelper().execute(context, () -> {

            Instant start = Instant.now();

            List<SimilarityRow>  simcenRows = new ArrayList<>();


            IntStream.range(1,188).forEach(id -> {

                LOG.info("Reviewing centroid '" + id + "'");

                Centroid centroid = new Centroid();
                centroid.setId(Long.valueOf(id));

                DataFrame refDF = context.getCassandraSQLContext()
                        .read()
                        .format("org.apache.spark.sql.cassandra")
                        .schema(DataTypes
                                .createStructType(new StructField[]{
                                        DataTypes.createStructField(ClusterDao.URI, DataTypes.StringType, false),
                                        DataTypes.createStructField(ClusterDao.CLUSTER, DataTypes.LongType, false)
                                }))
                        .option("inferSchema", "false") // Automatically infer data types
                        .option("charset", "UTF-8")
                        .option("mode", "DROPMALFORMED")
                        .options(ImmutableMap.of("table", ClusterDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                        .load()
                        .repartition(context.getRecommendedPartitions())
                        .filter( ClusterDao.CLUSTER +" = " + centroid.getId())
                        .persist(helper.getCacheModeHelper().getLevel());
                        ;
                long centroidSize = refDF.count();
                LOG.debug(centroidSize + " elements in cluster: " + centroid);

                boolean matched = false;

                for (int nid = 1; nid < 188; nid ++){

                    if ((id == nid) || (nid < id)) continue;

                    DataFrame neighbourDF = context.getCassandraSQLContext()
                            .read()
                            .format("org.apache.spark.sql.cassandra")
                            .schema(DataTypes
                                    .createStructType(new StructField[]{
                                            DataTypes.createStructField(ClusterDao.URI, DataTypes.StringType, false),
                                            DataTypes.createStructField(ClusterDao.CLUSTER, DataTypes.LongType, false)
                                    }))
                            .option("inferSchema", "false") // Automatically infer data types
                            .option("charset", "UTF-8")
                            .option("mode", "DROPMALFORMED")
                            .options(ImmutableMap.of("table", ClusterDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                            .load()
                            .repartition(context.getRecommendedPartitions())
                            .filter( ClusterDao.CLUSTER +" = " + nid)
                            .persist(helper.getCacheModeHelper().getLevel());
                    long neighbourSize = neighbourDF.count();

                    long intersection = refDF.select(ClusterDao.URI).intersect(neighbourDF.select(ClusterDao.URI)).count();

//                    neighbourDF.unpersist();

                    if (intersection>0){
                        LOG.info(intersection + " elements in both clusters: '" + id + "' and '" + nid+"'");
                        matched = true;

                        // From centroid to neighbour
                        SimilarityRow row1 = new SimilarityRow();
                        row1.setDate(TimeUtils.asISO());
                        row1.setResource_uri_1(String.valueOf(id));
                        row1.setResource_type_1("centroid");
                        row1.setResource_uri_2(String.valueOf(nid));
                        row1.setResource_type_2("centroid");
                        row1.setScore((intersection == 0)? 0.0 : Double.valueOf(intersection)/Double.valueOf(centroidSize));
                        simcenRows.add(row1);
                        LOG.debug("Created similarity: " + row1);

                        // From centroid to neighbour
                        SimilarityRow row2 = new SimilarityRow();
                        row2.setDate(TimeUtils.asISO());
                        row2.setResource_uri_1(String.valueOf(nid));
                        row2.setResource_type_1("centroid");
                        row2.setResource_uri_2(String.valueOf(id));
                        row2.setResource_type_2("centroid");
                        row2.setScore((intersection == 0)? 0.0 : Double.valueOf(intersection)/Double.valueOf(neighbourSize));
                        simcenRows.add(row2);
                        LOG.debug("Created similarity: " + row2);


                    }
                }



                if (!matched) LOG.warn("Cluster '" + centroid.getId() + "' has not intersections!!!");

//                refDF.unpersist();

            });

            Instant end1 = Instant.now();
            LOG.info("Calculated in: "       + ChronoUnit.HOURS.between(start,end1) + "h " +  ChronoUnit.MINUTES.between(start,end1) + "min " + (ChronoUnit.SECONDS.between(start,end1)%60) + "secs");

            LOG.info("ready to save " + simcenRows.size() + " similarities!!");

            JavaRDD<SimilarityRow> centroidRows = context.getSparkContext().parallelize(simcenRows);

            LOG.debug("saving centroid-similarities ..");
                context.getSqlContext()
                        .createDataFrame(centroidRows, SimilarityRow.class)
                        .write()
                        .format("org.apache.spark.sql.cassandra")
                        .options(ImmutableMap.of("table", SimilaritiesDao.CENTROIDS_TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                        .mode(SaveMode.Append)
                        .save();

            Instant end2 = Instant.now();
            LOG.info("Completed in: "       + ChronoUnit.HOURS.between(start,end2) + "h " +  ChronoUnit.MINUTES.between(start,end2) + "min " + (ChronoUnit.SECONDS.between(start,end2)%60) + "secs");

            });
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
}
