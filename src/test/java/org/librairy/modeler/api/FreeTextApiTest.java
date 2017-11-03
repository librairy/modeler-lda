/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.api;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.modeler.lda.Application;
import org.librairy.modeler.lda.api.FreeTextAPI;
import org.librairy.modeler.lda.models.SimilarResource;
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
@ContextConfiguration(classes = Application.class)
@TestPropertySource(properties = {
        "librairy.computing.fs = hdfs://minetur.dia.fi.upm.es:9000"
})
public class FreeTextApiTest {

    private static final Logger LOG = LoggerFactory.getLogger(FreeTextApiTest.class);

    @Autowired
    FreeTextAPI api;

    @Test
    public void similarResources() throws DataNotFound, InterruptedException {

        String domainUri    = "http://librairy.org/domains/eahb";

        Double minScore     = 0.7;
        Integer maxResults  = 10;
        List<String> types  = Arrays.asList(new String[]{"item"});

        Text text           = new Text();
        text.setId("sample-test");
        text.setContent("The processes are analyzed from different perspectives within different contexts, notably in the fields of linguistics, anesthesia, neuroscience, psychiatry, psychology, education, philosophy, anthropology, biology, systemics, logic, and computer science. These and other different approaches to the analysis of cognition are synthesised in the developing field of cognitive science, a progressively autonomous academic discipline. Within psychology and philosophy, the concept of cognition is closely related to abstract concepts such as mind and intelligence. It encompasses the mental functions, mental processes (thoughts), and states of intelligent entities (humans, collaborative groups, human organizations, highly autonomous machines, and artificial intelligences)");

        List<SimilarResource> similarResources = api.getSimilarResourcesTo(text, domainUri, minScore, maxResults, types);

        for (SimilarResource resource : similarResources){
            LOG.info("Similar Resource: " + resource);
        }

        LOG.info("completed!");

    }

}
