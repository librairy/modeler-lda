package org.librairy.modeler.builder;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.model.domain.resources.Domain;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.LDABuilder;
import org.librairy.modeler.lda.builder.TopicsBuilder;
import org.librairy.modeler.lda.models.TopicDescription;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.storage.UDM;
import org.librairy.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * Created on 27/06/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
        "librairy.lda.topic.words = 10"
})
public class TopicsBuilderTest {


    private static final Logger LOG = LoggerFactory.getLogger(TopicsBuilderTest.class);

    @Autowired
    LDABuilder ldaBuilder;

    @Autowired
    TopicsBuilder topicsBuilder;

    @Autowired
    UDM udm;

    String domainURI = "http://librairy.org/domains/4f56ab24bb6d815a48b8968a3b157470";


    @Test
    public void describe(){

        String domainId = URIGenerator.retrieveId(domainURI);

        TopicModel topicModel = ldaBuilder.load(domainId);

        List<TopicDescription> topics = topicsBuilder.retrieve(topicModel);

        topics.forEach(topic -> {
            LOG.info("Topic => " + topic);
        });

    }

    @Test
    public void persist(){

        String domainId = URIGenerator.retrieveId(domainURI);

        Instant start = Instant.now();

        Domain domain = Resource.newDomain("test");
        domain.setUri(domainURI);
        udm.save(domain);

        TopicModel topicModel = ldaBuilder.load(domainId);

        Map<String, String> topicRegistry = topicsBuilder.persist(topicModel);

        Instant end = Instant.now();

        LOG.info("Elapsed Time: " + ChronoUnit.MINUTES.between(start,end) + "min " + (ChronoUnit.SECONDS.between(start,end)%60) + "secs");

        LOG.info(topicRegistry.size() + " topics saved!");

    }


    @Test
    public void clean(){

        topicsBuilder.delete(domainURI);

        LOG.info("topics deleted!");

    }
}
