/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.services;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.models.Path;
import org.librairy.modeler.lda.services.SimilarityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by cbadenes on 11/01/16.
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
public class SimilarityServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityServiceTest.class);

    @Autowired
    SimilarityService service;

    @Test
    public void shortestPath() throws InterruptedException, IllegalArgumentException {

        String startUri     = "http://librairy.org/parts/5227bb3bbfbe38d7288b456c";
        String endUri       = "http://librairy.org/parts/546df640141674473e8b45bc";
        String domainUri    = "http://librairy.org/domains/default";

        Path result = service.getShortestPathBetween(startUri, endUri, 0.9, 10, domainUri);

        LOG.info("discovery path: " + result);

        result.getNodes().forEach(node -> LOG.info("Node: " + node));



    }

}
