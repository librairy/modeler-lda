/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.tasks;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.model.domain.resources.Domain;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.CorpusBuilder;
import org.librairy.modeler.lda.builder.DealsBuilder;
import org.librairy.modeler.lda.builder.LDABuilder;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Comparison;
import org.librairy.modeler.lda.models.Field;
import org.librairy.modeler.lda.tasks.LDAComparisonTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/**
 * Created on 27/06/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
//@TestPropertySource(properties = {
//        "librairy.lda.event.delay = 60000"
//})
public class LDAComparisonTaskTest {


    private static final Logger LOG = LoggerFactory.getLogger(LDAComparisonTaskTest.class);

    @Autowired
    ModelingHelper helper;

    @Test
    public void execute(){

//        Double minScore = 0.0;
//        Integer maxWords = 100;
//        List<String> domains = Arrays.asList( new String[]{
//                "http://librairy.org/domains/d0fb963ff976f9c37fc81fe03c21ea7b",
//                "http://librairy.org/domains/4ba29b9f9e5732ed33761840f4ba6c53"
//        });
//
//        Instant a = Instant.now();
//        List<Comparison<Field>> comparisons = new
//                LDAComparisonTask(helper).compareTopics(domains, maxWords, minScore);
//
//        Instant b = Instant.now();
//
//        System.out.println("Elapsed time: " + Duration.between(a,b).toMillis() + "msecs");
//
////        comparisons.forEach( comparison -> LOG.info("Comparison: " + comparison));

    }

    @Test
    public void executeTask() throws InterruptedException {

        //String domainUri = "http://librairy.org/domains/d0fb963ff976f9c37fc81fe03c21ea7b";
        String domainUri = "http://librairy.org/domains/4ba29b9f9e5732ed33761840f4ba6c53";


        helper.getDomainsDao().listOnly(Domain.URI).forEach(uri -> {
            helper.getComparisonsDao().initialize(uri);

            Instant a = Instant.now();
            new LDAComparisonTask(uri, helper).run();
            Instant b = Instant.now();
            System.out.println("Elapsed time: " + Duration.between(a,b).toMillis());
        });

        LOG.info("sleeping..");
        Thread.sleep(Long.MAX_VALUE);
    }

}
