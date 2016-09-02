package org.librairy.modeler.optimizer;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.CorpusBuilder;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.optimizers.LDAOptimizer;
import org.librairy.modeler.lda.optimizers.LDAParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created on 01/09/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
        "librairy.columndb.host = wiener.dia.fi.upm.es",
        "librairy.columndb.port = 5011",
        "librairy.documentdb.host = wiener.dia.fi.upm.es",
        "librairy.documentdb.port = 5021",
        "librairy.graphdb.host = wiener.dia.fi.upm.es",
        "librairy.graphdb.port = 5030",
        "librairy.eventbus.host = local",
        "librairy.lda.maxiterations = 3",
        "librairy.lda.optimizer = nsga",
})
public class NSGAOptimizerTest {

    private static final Logger LOG = LoggerFactory.getLogger(NSGAOptimizerTest.class);

    String domainURI = "http://drinventor.eu/domains/4f56ab24bb6d815a48b8968a3b157470";

    @Autowired
    CorpusBuilder corpusBuilder;

    @Autowired
    LDAOptimizer optimizer;

    @Test
    public void buildByDomain(){

        Corpus corpus = corpusBuilder.build(domainURI, Resource.Type.ITEM);

        LDAParameters parameters = optimizer.getParametersFor(corpus);

        LOG.info("Parameters: " + parameters);

    }

}
