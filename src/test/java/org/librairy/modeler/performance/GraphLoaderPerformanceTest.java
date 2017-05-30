/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.performance;

import com.google.common.collect.ImmutableMap;
import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.Config;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.computing.helper.ComputingHelper;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.dao.SimilaritiesDao;
import org.librairy.modeler.lda.functions.RowToTupleLongVector;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import scala.Tuple2;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
        "librairy.computing.fs = local",
        "librairy.uri = librairy.linkeddata.es/resources"
})
public class GraphLoaderPerformanceTest {

    private static final Logger LOG = LoggerFactory.getLogger(GraphLoaderPerformanceTest.class);

    @Autowired
    StorageHelper storageHelper;

    @Autowired
    URIGenerator uriGenerator;

    @Autowired
    ComputingHelper computingHelper;

    @Autowired
    ModelingHelper helper;



    StructType edgeDataType = DataTypes
            .createStructType(new StructField[]{
                    DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType, false),
                    DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType, false),
                    DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType, false),
                    DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_1, DataTypes.StringType, false),
                    DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_2, DataTypes.StringType, false)
            });

    StructType shapesDataType = DataTypes
            .createStructType(new StructField[]{
                    DataTypes.createStructField(ShapesDao.RESOURCE_ID, DataTypes.StringType, false),
                    DataTypes.createStructField(ShapesDao.DATE, DataTypes.StringType, false),
                    DataTypes.createStructField(ShapesDao.RESOURCE_TYPE, DataTypes.StringType, false),
                    DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
                    DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
            });

    @Test
    public void saveToCVS() throws InterruptedException {

        Instant startG = Instant.now();
        ComputingContext context = computingHelper.newContext("sample");

        String domainUri = "http://librairy.linkeddata.es/resources/domain/patents";

        DataFrame df = context.getCassandraSQLContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(edgeDataType)
                .option("inferSchema", "false")
                .option("charset", "UTF-8")
                .option("mode", "DROPMALFORMED")
                .options(ImmutableMap.of("table", SimilaritiesDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                .load()
                .repartition(context.getRecommendedPartitions()/2);

        DataFrame df2 = context.getCassandraSQLContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(shapesDataType)
                .option("inferSchema", "false")
                .option("charset", "UTF-8")
                .option("mode", "DROPMALFORMED")
                .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", "lda_patents"))
                .load()
                .repartition(context.getRecommendedPartitions()/2);


        DataFrame distTopicsDF = df.join(df2, df.col(SimilaritiesDao.RESOURCE_URI_1).equalTo(df2.col(ShapesDao.RESOURCE_URI)));

        LOG.info("Saving to cvs");
        df      .write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("/librairy/graphs/patent-similarities-8-ex.csv");

        LOG.info("Total elapsed time: " + Duration.between(startG, Instant.now()).toMillis() + " msecs");

    }

    @Test
    public void calculateAndSaveToCVS() throws InterruptedException {

        Instant startG = Instant.now();
        ComputingContext context = computingHelper.newContext("sample");

        String domainUri = "http://librairy.linkeddata.es/resources/domain/patents";

        JavaRDD<Tuple2<Long,Vector>> vectors = context.getCassandraSQLContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField(ShapesDao.RESOURCE_ID, DataTypes.LongType, false),
                                DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
                        }))
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
//                        .option("spark.sql.autoBroadcastJoinThreshold","-1")
                .option("mode", "DROPMALFORMED")
                .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                .load()
                .repartition(context.getRecommendedPartitions()/2)
                .toJavaRDD()
                .map(new RowToTupleLongVector())
                .persist(helper.getCacheModeHelper().getLevel());
        vectors.take(1);

        LOG.info("Calculating similarities");
        JavaRDD<Row> simRows = vectors
                .cartesian(vectors)
                .filter( p -> !p._1._1.equals(p._2._1))
                .filter( p -> JensenShannonSimilarity.apply(p._1._2.toArray(), p._2._2.toArray()) > 0.5)
                .map(sr -> RowFactory.create(sr._1._1, sr._2._1))
                ;

        StructType shapesDataType = DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField("id1", DataTypes.LongType, false),
                        DataTypes.createStructField("id2", DataTypes.LongType, false)
                });
        LOG.info("Creating output");
        DataFrame dfout = context.getCassandraSQLContext().createDataFrame(simRows, shapesDataType);


        dfout   .write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("/librairy/graphs/patent-similarities-8-ex.csv");

        LOG.info("Total elapsed time: " + Duration.between(startG, Instant.now()).toMillis() + " msecs");

    }


    private void saveData(Integer partitions, DataFrame dataFrame) {
        try {
            Instant start = Instant.now();
            String path = "/librairy/tests/data-" + partitions;
            storageHelper.deleteIfExists(path);

            // Save the model
            dataFrame
                    .repartition(partitions)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .save(path);
            LOG.info("Saved at "+path  + " in " + Duration.between(start, Instant.now()).toMillis() + "msecs");

        }catch (Exception e){
            if (e instanceof java.nio.file.FileAlreadyExistsException) {
                LOG.warn(e.getMessage());
            }else {
                LOG.error("Error saving model", e);
            }
        }
    }


    private void loadData(Integer partitions, ComputingContext context){
        try {
            Instant start = Instant.now();
            String path = "/librairy/tests/data-" + partitions;
            storageHelper.deleteIfExists(path);

            // Save the model
            DataFrame dataframe = context.getSqlContext()
                    .read()
                    .schema(edgeDataType)
                    .load(path);
//                    .repartition(partitions);

            dataframe.count();

            LOG.info("Loaded from "+path  + " in " + Duration.between(start, Instant.now()).toMillis() + "msecs");

        }catch (Exception e){
            if (e instanceof java.nio.file.FileAlreadyExistsException) {
                LOG.warn(e.getMessage());
            }else {
                LOG.error("Error saving model", e);
            }
        }
    }

    @Test
    public void read() throws InterruptedException {

        ComputingContext context = computingHelper.newContext("sample");

        List<Integer> sample = Arrays.asList(new Integer[]{
                1,
                2,
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors()*2,
                context.getRecommendedPartitions(),
                100,
                500,
                1000
        });

        List<Integer> partitions = sample;//Lists.reverse(sample);
        loadData(partitions.get(0), context);

        for (Integer partition : partitions){
            loadData(partition, context);
        }


    }

}
