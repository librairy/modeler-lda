/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.api;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.modeler.lda.api.ApiConfig;
import org.librairy.modeler.lda.api.FreeTextAPI;
import org.librairy.modeler.lda.api.ShortestPathAPI;
import org.librairy.modeler.lda.models.Path;
import org.librairy.modeler.lda.models.SimilarResource;
import org.librairy.modeler.lda.models.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ApiConfig.class)
@TestPropertySource(properties = {
        "librairy.computing.fs = hdfs://minetur.dia.fi.upm.es:9000",
        "librairy.computing.cluster = spark://minetur.dia.fi.upm.es:7077",
        "librairy.computing.cores = 24",
        "librairy.computing.memory = 48g"
})
public class ShortestPathApiTest {

    private static final Logger LOG = LoggerFactory.getLogger(ShortestPathApiTest.class);

    @Autowired
    ShortestPathAPI api;

    @Test
    public void similarResources() throws DataNotFound, InterruptedException {

        String domainUri    = "http://librairy.linkeddata.es/resources/domains/blueBottle";
        String startUri     = "http://librairy.linkeddata.es/resources/items/wv_M6xPFrin";
        String endUri       = "http://librairy.linkeddata.es/resources/parts/52fd1a0934b2ae59248b4938";
        Double minScore     = 0.5;
        Integer maxResults  = 5;
        Integer maxLength   = 20;
        List<String> types  = Collections.emptyList();Arrays.asList(new String[]{"item"});


        Instant start = Instant.now();
        List<Path> paths = api.calculate(startUri, endUri, domainUri, minScore, maxLength, maxResults, types);
        LOG.info("Elapsed time: " + Duration.between(start, Instant.now()).toMillis() + " msecs");

        for (Path resource : paths){
            LOG.info("Path: " + resource);
        }

        LOG.info("completed!");

    }

}
