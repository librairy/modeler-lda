package org.librairy.modeler.builder;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.CorpusBuilder;
import org.librairy.modeler.lda.builder.LDABuilder;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
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
@TestPropertySource(properties = {
        "librairy.columndb.host = wiener.dia.fi.upm.es",
        "librairy.columndb.port = 5011",
        "librairy.documentdb.host = wiener.dia.fi.upm.es",
        "librairy.documentdb.port = 5021",
        "librairy.graphdb.host = wiener.dia.fi.upm.es",
        "librairy.graphdb.port = 5030",
        "librairy.eventbus.host = local",
        "librairy.modeler.maxiterations = 3",
        "librairy.vocabulary.size = 10000"
})
public class LDABuilderTest {


    private static final Logger LOG = LoggerFactory.getLogger(LDABuilderTest.class);

    @Autowired
    LDABuilder ldaBuilder;

    @Autowired
    CorpusBuilder corpusBuilder;


    String domainURI = "http://drinventor.eu/domains/4f56ab24bb6d815a48b8968a3b157470";

    @Test
    public void buildByDomain(){

        Corpus corpus = corpusBuilder.build(domainURI, Resource.Type.ITEM);

        ldaBuilder.build(corpus);

    }

    @Test
    public void buildByUris(){

        List<String> uris = Arrays.asList(new String[]{
                "http://drinventor.eu/items/9c8b49fbc507cfe9903fc9f08dc2a8c8",
                "http://drinventor.eu/items/ec934613dfb9acddd68f89c579f24aff",
                "http://drinventor.eu/items/7da1c096e73093f6404cd28946d313a6",
                "http://drinventor.eu/items/71b295f128318469a3f04d25ae8b18c",
                "http://drinventor.eu/items/cc9ca0ab0fe51b6ec99c09a2cba75249",
                "http://drinventor.eu/items/2073b01f3c011e62903d27f8549167a8",
                "http://drinventor.eu/items/6cead06fa9b7ec59daf55d186f1085b3",
                "http://drinventor.eu/items/2906920d70fb1fc1a67d171245160e02",
                "http://drinventor.eu/items/548b6e832a232c101f8d9ab3a16d6d95",
                "http://drinventor.eu/items/d75ddc6e097d193a5cfee8396aab8e21"
        });

        Corpus corpus = corpusBuilder.build("test",uris);

        ldaBuilder.build(corpus);

    }

    @Test
    public void load(){

        String domainId = URIGenerator.retrieveId(domainURI);

        TopicModel topicModel = ldaBuilder.load(domainId);

        System.out.println(topicModel.getCountVectorizerModel().vocabulary()[20]);

    }

}
