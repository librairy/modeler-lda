package org.librairy.modeler.tasks;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.*;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.modeler.lda.tasks.LDACreationTask;
import org.librairy.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
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
        "librairy.lda.event.delay = 60000"
})
public class LDACreationTaskTest {


    private static final Logger LOG = LoggerFactory.getLogger(LDACreationTaskTest.class);

    @Autowired
    LDABuilder ldaBuilder;

    @Autowired
    CorpusBuilder corpusBuilder;

    @Autowired
    DealsBuilder dealsBuilder;

    @Autowired
    TopicsBuilder topicsBuilder;

    @Autowired
    SimilarityBuilder similarityBuilder;

    @Autowired
    URIGenerator uriGenerator;

    @Autowired
    ModelingHelper helper;

    String domainUri = "http://drinventor.eu/domains/4f56ab24bb6d815a48b8968a3b157470";

    @Test
    public void execute(){

        // Create corpus
        Corpus corpus = corpusBuilder.build(domainUri, Resource.Type.ITEM);

        // Create a new Topic Model
        TopicModel model = ldaBuilder.build(corpus);

        // Retrieve topics from Model
        ConcurrentHashMap<String,String> registry = new ConcurrentHashMap<>();
        topicsBuilder.retrieve(model).parallelStream().forEach(topicDescription -> {

            String content  = topicDescription.getWords().stream().map(wd -> wd.getWord()).collect(Collectors.joining(","));
            String uri      = uriGenerator.basedOnContent(Resource.Type.TOPIC,content);
            registry.put(topicDescription.getId(),uri);
        });

        // Relate documents to topics
        dealsBuilder.build(corpus,model,registry);

    }


    @Test
    public void executeOnUris(){

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

        // Create a new Topic Model
        TopicModel model = ldaBuilder.build(corpus);

        // Retrieve topics from Model
        ConcurrentHashMap<String,String> registry = new ConcurrentHashMap<>();
        topicsBuilder.retrieve(model).parallelStream().forEach(topicDescription -> {

            String content  = topicDescription.getWords().stream().map(wd -> wd.getWord()).collect(Collectors.joining(","));
            String uri      = uriGenerator.basedOnContent(Resource.Type.TOPIC,content);
            registry.put(topicDescription.getId(),uri);
        });

        // Relate documents to topics
        dealsBuilder.build(corpus,model,registry);

        // Relate parts to topics
        Corpus corpusOfParts = corpusBuilder.build(domainUri, Resource.Type.PART);
        corpusOfParts.setCountVectorizerModel(corpus.getCountVectorizerModel());
        dealsBuilder.build(corpusOfParts,model,registry);

    }

    @Test
    public void runTask() throws InterruptedException {

        String domainUri = "http://librairy.org/domains/default";
        LDACreationTask task = new LDACreationTask(domainUri, helper);
        task.run();

        LOG.info("################################## Task completed!!!!");
        long timeToSleep = 6000000;
        LOG.info("Waiting for: " + timeToSleep);
        Thread.currentThread().sleep(timeToSleep);
    }

    @Test
    public void buildSimilarityRelations(){
        //Calculate similarities based on the model
        String domainUri = "http://librairy.org/domains/default";
        helper.getSimilarityBuilder().discover(domainUri, Resource.Type.ITEM);
    }

}
