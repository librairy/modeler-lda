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
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.modeler.lda.api.ApiConfig;
import org.librairy.modeler.lda.api.LDAModelerAPI;
import org.librairy.modeler.lda.api.model.Criteria;
import org.librairy.modeler.lda.api.model.ScoredResource;
import org.librairy.modeler.lda.api.model.ScoredTopic;
import org.librairy.modeler.lda.api.model.ScoredWord;
import org.librairy.modeler.lda.models.Comparison;
import org.librairy.modeler.lda.models.Field;
import org.librairy.modeler.lda.models.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ApiConfig.class)
//@TestPropertySource(properties = {
//        "librairy.columndb.host = zavijava.dia.fi.upm.es",
//        "librairy.documentdb.host = zavijava.dia.fi.upm.es",
//        "librairy.graphdb.host = zavijava.dia.fi.upm.es",
//        "librairy.eventbus.host = zavijava.dia.fi.upm.es"
////        "librairy.uri = drinventor.eu" //librairy.org
//})
public class ApiTest {

    private static final Logger LOG = LoggerFactory.getLogger(ApiTest.class);

    @Autowired
    LDAModelerAPI api;

    @Test
    public void mostRelevantResources(){

        String topic = "http://librairy.org/topics/f5d024c8871719cd9ae0754d3f4ffbad";
        List<ScoredResource> resources = api.getMostRelevantResources
                (topic, new Criteria());

        LOG.info("Resources: " + resources);

    }


    @Test
    public void similarResourcesFromUri(){

        String uri = "http://librairy.org/parts/5227aa9dbfbe38ad1c8b457f";


        Criteria criteria = new Criteria();
        criteria.setMax(20);
        criteria.setThreshold(0.5);
//        criteria.setTypes(Arrays.asList(new Resource.Type[]{Resource.Type.ITEM}));

        List<ScoredResource> resources = api.getSimilarResources(uri,criteria);

        resources.forEach(res -> LOG.info("Resource: " + res));

    }

    @Test
    public void similarResourcesFromText(){

        Text text = new Text("sample","Remember, as you use CQL, that query planning is not meant to be one of its strengths. Cassandra code typically makes the assumption that you have huge amounts of data, so it will try to avoid doing any queries that might end up being expensive. In the RMDBS world, you structure your data according to intrinsic relationships (3rd normal form, etc), whereas in Cassandra, you structure your data according to the queries you expect to need. Denormalization is (forgive the pun) the norm.");
        List<ScoredResource> resources = api.getSimilarResources(text,new Criteria());

        resources.forEach(res -> LOG.info("Resource: " + res));
    }

    @Test
    public void compareDomains(){

        List<String> domains = Arrays.asList(new String[]{
           "http://librairy.org/domains/90b559119ab48e8cf4310bf92f6b4eab",
           "http://librairy.org/domains/d4067b8f01c5ea966a202774bdadea5c",
           "http://librairy.org/domains/634586c47c4f893ccd90ff23937e8548",
           "http://librairy.org/domains/default"
        });

        List<Comparison<Field>> comparisons = api
                .compareTopicsFrom(domains, new Criteria());


        comparisons.forEach(comp -> LOG.info("Comparison: " + comp));
    }


    @Test
    public void tags(){

        String documentUri = "http://librairy.org/documents/bpQYqx9C4HW";
        String itemUri = "http://librairy.org/items/fd798de0e08bdce63359a2074a799877";

        /**
         * http://librairy.org/parts/5227c498bfbe38d7288b5171
         http://librairy.org/parts/5227c498bfbe38d7288b5170
         http://librairy.org/parts/5227c498bfbe38d7288b516f
         http://librairy.org/parts/5227c498bfbe38d7288b516e
         http://librairy.org/parts/5227c498bfbe38d7288b516d
         http://librairy.org/parts/5227c498bfbe38d7288b516c
         http://librairy.org/parts/5227c498bfbe38d7288b516b
         http://librairy.org/parts/5227c498bfbe38d7288b516a
         http://librairy.org/parts/5227c498bfbe38d7288b5169
         http://librairy.org/parts/5227c498bfbe38d7288b5168
         http://librairy.org/parts/5227c498bfbe38d7288b5167
         http://librairy.org/parts/5227c498bfbe38d7288b5166
         http://librairy.org/parts/5227c498bfbe38d7288b5165
         http://librairy.org/parts/5227c498bfbe38d7288b5164
         http://librairy.org/parts/5227c498bfbe38d7288b5163
         http://librairy.org/parts/5227c498bfbe38d7288b5162
         http://librairy.org/parts/5227c498bfbe38d7288b5161
         http://librairy.org/parts/5227c498bfbe38d7288b5160
         http://librairy.org/parts/5227c498bfbe38d7288b515f
         http://librairy.org/parts/5227c498bfbe38d7288b515e
         */

        String resourceUri = "http://librairy.org/parts/5227c498bfbe38d7288b516c";
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
