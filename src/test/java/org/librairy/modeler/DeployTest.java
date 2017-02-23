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
import org.librairy.modeler.lda.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by cbadenes on 13/01/16.
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
//@TestPropertySource(properties = {
////        "librairy.computing.cores = 120",
////        "librairy.computing.memory = 84g",
////        "librairy.computing.fs = hdfs://minetur.dia.fi.upm.es:9000",
////        "librairy.computing.cluster = spark://minetur.dia.fi.upm.es:7077",
////        "librairy.lda.event.delay = 1000",
////        "librairy.columndb.host = zavijava.dia.fi.upm.es",
////        "librairy.documentdb.host = zavijava.dia.fi.upm.es",
////        "librairy.graphdb.host = zavijava.dia.fi.upm.es",
////        "librairy.eventbus.host = zavijava.dia.fi.upm.es"
////        "librairy.uri = drinventor.eu" //librairy.org
//})
public class DeployTest {

    private static final Logger LOG = LoggerFactory.getLogger(DeployTest.class);

    @Test
    public void run() throws InterruptedException {

        LOG.info("Sleepping...");
        Thread.sleep(Integer.MAX_VALUE);
        LOG.info("Wake Up!");
    }
}


