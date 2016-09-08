package org.librairy.modeler.services;

import es.cbadenes.lab.test.IntegrationTest;
import org.librairy.modeler.lda.Config;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.modeler.lda.services.ModelingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by cbadenes on 11/01/16.
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
public class ModelingServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ModelingServiceTest.class);

    @Autowired
    ModelingService service;

    @Test
    public void scheduleModelingTasks() throws InterruptedException {

        String domainUri = "http://drinventor.eu/domains/7df34748-7fad-486e-a799-3bcd86a03499";

        service.train(domainUri,1000);
        Thread.sleep(1000);
        service.train(domainUri,1000);
        service.train(domainUri,1000);
        Thread.sleep(1000);
        service.train(domainUri,1000);
        service.train(domainUri,1000);
        Thread.sleep(1000);

        LOG.info("waiting for execution....");
        Thread.sleep(10000);


    }

    @Test
    public void buildModel() throws InterruptedException {
        String domainUri = "http://librairy.org/domains/default";
        service.train(domainUri,1000);

        LOG.info("################################## Task completed!!!!");
        long timeToSleep = 6000000;
        LOG.info("Waiting for: " + timeToSleep);
        Thread.currentThread().sleep(timeToSleep);
    }
}
