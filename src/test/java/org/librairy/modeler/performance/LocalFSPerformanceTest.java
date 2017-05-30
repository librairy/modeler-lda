/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.performance;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.api.java.JavaRDD;
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
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.Config;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.computing.helper.ComputingHelper;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.modeler.lda.dao.SimilaritiesDao;
import org.librairy.modeler.lda.dao.SimilarityRow;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
        "librairy.computing.fs = local"
})
public class LocalFSPerformanceTest {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFSPerformanceTest.class);

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

    @Test
    public void save() throws InterruptedException {

        Instant startG = Instant.now();
        ComputingContext context = computingHelper.newContext("sample");

        List<Integer> elements = IntStream.range(1, 2000).boxed().collect(Collectors.toList());

        JavaRDD<Integer> rdd = context.getSparkContext().parallelize(elements);

        JavaRDD<SimilarityRow> simRows = rdd
                .cartesian(rdd)
                .repartition(context.getRecommendedPartitions())
                .map(pair -> {
                    SimilarityRow row1 = new SimilarityRow();
                    row1.setResource_uri_1(String.valueOf(pair._1));
                    row1.setResource_uri_2(String.valueOf(pair._2));
                    row1.setScore(pair._1.doubleValue() + pair._2.doubleValue());
                    row1.setResource_type_1(Resource.Type.ITEM.key());
                    row1.setResource_type_2(Resource.Type.ITEM.key());
                    row1.setDate(TimeUtils.asISO());
                    return row1;

                })
                .persist(helper.getCacheModeHelper().getLevel());
        LOG.info("calculating similarities btw documents ");
        simRows.take(1);




        LOG.info("saving subgraph edges from sector ");
        JavaRDD<Row> rows = simRows.map(sr -> RowFactory.create(sr.getResource_uri_1(), sr.getResource_uri_2(), sr.getScore(), sr.getResource_type_1(), sr.getResource_type_2()));

        DataFrame dataframe = context
                .getSqlContext()
                .createDataFrame(rows, edgeDataType)
                .persist(helper.getCacheModeHelper().getLevel());

        dataframe.take(1);

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
        saveData(partitions.get(0),dataframe);

        for (Integer partition : partitions){
            saveData(partition, dataframe);
        }


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
