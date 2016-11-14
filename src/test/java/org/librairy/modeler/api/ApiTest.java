/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.api;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.modeler.lda.api.ApiConfig;
import org.librairy.modeler.lda.api.LDAModelerAPI;
import org.librairy.modeler.lda.api.model.Criteria;
import org.librairy.modeler.lda.api.model.ScoredResource;
import org.librairy.modeler.lda.api.model.ScoredTopic;
import org.librairy.modeler.lda.api.model.ScoredWord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ApiConfig.class)
public class ApiTest {

    private static final Logger LOG = LoggerFactory.getLogger(ApiTest.class);

    @Autowired
    LDAModelerAPI api;

    @Test
    public void mostRelevantResources(){

        String topic = "http://librairy.org/topics/4b63ec0843be247f948d16d5947da581";
        List<ScoredResource> resources = api.getMostRelevantResources
                (topic, new Criteria());

        LOG.info("Resources: " + resources);

    }


    @Test
    public void tags(){

        String resourceUri = "http://librairy.org/parts/5227bd0abfbe38d7288b4786";
        List<ScoredWord> tags = api.getTags(resourceUri, new Criteria());

        tags.forEach(tag -> LOG.info("Tag: " + tag));

    }

    @Test
    public void topicsDistribution(){

        String resourceUri = "http://librairy.org/parts/5227bd0abfbe38d7288b4786";
        List<ScoredTopic> tags = api.getTopicsDistribution(resourceUri, new Criteria());

        tags.forEach(el -> LOG.info("Topic: " + el));

    }
}
