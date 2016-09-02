package org.librairy.modeler.services;

import es.cbadenes.lab.test.IntegrationTest;
import org.librairy.model.domain.relations.Contains;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.modeler.lda.services.TopicModelingService;
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
@TestPropertySource(properties = {
        "librairy.modeler.delay = 5000"
})
public class ModelingServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ModelingServiceTest.class);

    @Autowired
    TopicModelingService service;

    @Test
    public void scheduleModelingTasks() throws InterruptedException {

        String domainUri = "http://drinventor.eu/domains/7df34748-7fad-486e-a799-3bcd86a03499";

        service.buildModels(domainUri, Resource.Type.ITEM);
        Thread.sleep(1000);
        service.buildModels(domainUri, Resource.Type.ITEM);
        service.buildModels(domainUri, Resource.Type.ITEM);
        Thread.sleep(1000);
        service.buildModels(domainUri, Resource.Type.ITEM);
        service.buildModels(domainUri, Resource.Type.ITEM);
        Thread.sleep(1000);

        LOG.info("waiting for execution....");
        Thread.sleep(10000);


    }
}
