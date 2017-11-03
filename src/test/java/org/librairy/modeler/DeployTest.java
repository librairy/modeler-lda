/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.modeler.lda.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by cbadenes on 13/01/16.
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@PropertySource({"classpath:boot.properties","classpath:computing.properties", "classpath:application.properties"})
public class DeployTest {

    private static final Logger LOG = LoggerFactory.getLogger(DeployTest.class);

    @Test
    public void run() throws InterruptedException {

        LOG.info("Sleepping...");
        Thread.sleep(Integer.MAX_VALUE);
        LOG.info("Wake Up!");
    }
}


