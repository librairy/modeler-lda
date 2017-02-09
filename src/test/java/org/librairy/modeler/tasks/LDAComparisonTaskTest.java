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
import org.librairy.metrics.distance.ExtendedKendallsTauSimilarity;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.CorpusBuilder;
import org.librairy.modeler.lda.builder.DealsBuilder;
import org.librairy.modeler.lda.builder.LDABuilder;
import org.librairy.modeler.lda.dao.TopicRank;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Comparison;
import org.librairy.modeler.lda.models.Field;
import org.librairy.modeler.lda.tasks.LDAComparisonTask;
import org.librairy.modeler.lda.utils.LevenshteinSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 27/06/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
//        "librairy.computing.cores = 120",
//        "librairy.computing.memory = 84g",
//        "librairy.computing.fs = hdfs://minetur.dia.fi.upm.es:9000",
//        "librairy.computing.cluster = spark://minetur.dia.fi.upm.es:7077",
        "librairy.lda.event.delay = 1000",
//        "librairy.columndb.host = zavijava.dia.fi.upm.es",
//        "librairy.documentdb.host = zavijava.dia.fi.upm.es",
//        "librairy.graphdb.host = zavijava.dia.fi.upm.es",
        "librairy.eventbus.host = local"
//        "librairy.uri = drinventor.eu" //librairy.org
})
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
        String domainUri = "http://librairy.org/domains/ii";
        new LDAComparisonTask(domainUri, helper).run();


//        helper.getDomainsDao().listOnly(Domain.URI).forEach(uri -> {
//            helper.getComparisonsDao().initialize(uri);
//
//            Instant a = Instant.now();
//            new LDAComparisonTask(uri, helper).run();
//            Instant b = Instant.now();
//            System.out.println("Elapsed time: " + Duration.between(a,b).toMillis());
//        });
//
        LOG.info("sleeping..");
        Thread.sleep(Long.MAX_VALUE);
    }


    @Test
    public void single(){

        List<TopicRank> r1 = helper.getTopicsDao().listAsRank("http://librairy.org/domains/ii", 100);
        List<TopicRank> r2 = helper.getTopicsDao().listAsRank("http://librairy.org/domains/em", 100);


        for (TopicRank t1 : r1){

            for (TopicRank t2: r2){


                Double score = new ExtendedKendallsTauSimilarity<String>().calculate(t1.getWords(), t2.getWords(), new LevenshteinSimilarity());
//                LOG.info("========================================");
//                LOG.info("T1: " + t1.getWords().getPairs().stream().sorted((a,b) -> -a._2.compareTo(b._2)).limit(20).map(r -> r._1 + "(" + r._2+")").collect(Collectors.joining(" ")));
//                LOG.info("T2: " + t2.getWords().getPairs().stream().sorted((a,b) -> -a._2.compareTo(b._2)).limit(20).map(r -> r._1 + "(" + r._2+")").collect(Collectors.joining(" ")));
                LOG.info("Score: " + score);

                if (score > 1.0){
                    LOG.error("Error!!! " + score);
                }

            }


        }



    }
}
