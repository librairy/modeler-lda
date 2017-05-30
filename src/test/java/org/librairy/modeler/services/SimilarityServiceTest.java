/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.services;

import com.google.common.collect.ImmutableMap;
import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.api.LDAModelerAPI;
import org.librairy.modeler.lda.api.model.Criteria;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
        "librairy.lda.optimizer = basic",
        "librairy.lda.maxevaluations = 10",
        "librairy.computing.cluster = spark://minetur.dia.fi.upm.es:7077",
        "librairy.computing.cores = 120",
        "librairy.computing.fs = hdfs://minetur.dia.fi.upm.es:9000"
})
public class SimilarityServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityServiceTest.class);

    @Autowired
    ModelingHelper helper;


    @Autowired
    LDAModelerAPI api;


    @Test
    public void shortestPath() throws InterruptedException, DataNotFound {
        String startUri = "http://librairy.org/items/QPp_13RU16Sv1";
        String endUri   = "http://librairy.org/items/XyAE13Ei2RhQP";
        //List<String> types = Arrays.asList(new String[]{"item"});
        List<String> types = Collections.emptyList();
        Integer maxLength = 10;
        Criteria criteria = new Criteria();
        criteria.setDomainUri("http://librairy.org/domains/ae5753952f7db4b1d56a5942e08476f9");
        criteria.setMax(10);
        criteria.setThreshold(0.5);
        List<Path> path = api.getShortestPath(startUri, endUri, types, maxLength, criteria, 5);

        if (path != null){
            path.forEach(p -> LOG.info("Path -> " + p));
        }

//        LOG.info("sleeping...");
//        Thread.sleep(Long.MAX_VALUE);

    }

    @Test
    public void saveCentroids(){
        final String domainUri = "http://librairy.org/domains/ae5753952f7db4b1d56a5942e08476f9";
        try {
            final ComputingContext context = helper.getComputingHelper().newContext("lda.api.centroids.test");

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
                    .options(ImmutableMap.of("table", ShapesDao.CENTROIDS_TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda", URIGenerator.retrieveId(domainUri))))
                    .load();
//                    .repartition(context.getRecommendedPartitions())
//                    .cache();

            LOG.info("Saving centroids in filesystem ...");
            helper.getSimilarityService().saveCentroids(context, domainUri, dataFrame);
        }catch (Exception e){
            LOG.error("Unexpected error", e);
        };
    }

}
