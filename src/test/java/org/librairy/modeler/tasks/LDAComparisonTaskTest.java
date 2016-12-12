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
    LDABuilder ldaBuilder;

    @Autowired
    CorpusBuilder corpusBuilder;

    @Autowired
    DealsBuilder dealsBuilder;

    @Autowired
    URIGenerator uriGenerator;

    @Autowired
    ModelingHelper helper;

    String domainUri = "http://drinventor.eu/domains/4f56ab24bb6d815a48b8968a3b157470";

    @Test
    public void execute(){

        Double minScore = 0.0;
        Integer maxWords = 10;
        List<String> domains = Arrays.asList( new String[]{
                "http://librairy.org/domains/90b559119ab48e8cf4310bf92f6b4eab",
                "http://librairy.org/domains/d4067b8f01c5ea966a202774bdadea5c",
                "http://librairy.org/domains/634586c47c4f893ccd90ff23937e8548"
        });

        List<Comparison<Field>> comparisons = new
                LDAComparisonTask(helper).compareTopics(domains, maxWords, minScore);


        comparisons.forEach( comparison -> LOG.info("Comparison: " + comparison));

    }

}
