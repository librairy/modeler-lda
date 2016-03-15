package org.librairy.modeler;

import es.cbadenes.lab.test.IntegrationTest;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.topic.TopicModeler;
import org.librairy.storage.UDM;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by cbadenes on 13/01/16.
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
        "librairy.comparator.delay = 1000",
        "librairy.cassandra.contactpoints = zavijava.dia.fi.upm.es",
        "librairy.cassandra.port = 5011",
        "librairy.cassandra.keyspace = research",
        "librairy.elasticsearch.contactpoints = zavijava.dia.fi.upm.es",
        "librairy.elasticsearch.port = 5021",
        "librairy.neo4j.contactpoints = zavijava.dia.fi.upm.es",
        "librairy.neo4j.port = 5032",
        "librairy.eventbus.uri = amqp://librairy:drinventor@drinventor.dia.fi.upm.es:5041/drinventor"})
public class FixModelingTest {

    private static final Logger LOG = LoggerFactory.getLogger(FixModelingTest.class);

    @Autowired
    ModelingHelper helper;

    @Autowired
    UDM udm;


    @Test
    public void topicModel() throws InterruptedException {

        String domainUri = "http://drinventor.eu/domains/7df34748-7fad-486e-a799-3bcd86a03499";
        new TopicModeler(domainUri,helper, Resource.Type.ITEM).run();

    }



}
