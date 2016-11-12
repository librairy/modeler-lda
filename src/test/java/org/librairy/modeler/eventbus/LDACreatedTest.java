/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.eventbus;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.model.Event;
import org.librairy.model.modules.EventBus;
import org.librairy.model.modules.RoutingKey;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.services.ModelingService;
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
public class LDACreatedTest {

    private static final Logger LOG = LoggerFactory.getLogger(LDACreatedTest.class);

    @Autowired
    EventBus eventBus;

    @Test
    public void defaultDomain() throws InterruptedException {

        String domainUri = "http://librairy.org/domains/default";

        eventBus.post(Event.from(domainUri), RoutingKey.of("lda.created"));

        LOG.info("Sleepping...");
        Thread.sleep(Integer.MAX_VALUE);
        LOG.info("Wake Up!");

    }


}
