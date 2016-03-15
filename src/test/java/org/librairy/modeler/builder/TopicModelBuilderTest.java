package org.librairy.modeler.builder;

import es.cbadenes.lab.test.IntegrationTest;
import es.upm.oeg.epnoi.matching.metrics.domain.entity.RegularResource;
import org.librairy.model.domain.relations.DealsWithFromItem;
import org.librairy.model.domain.relations.MentionsFromTopic;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.*;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.RegularResourceBuilder;
import org.librairy.modeler.lda.builder.TopicModelBuilder;
import org.librairy.modeler.lda.models.WordDistribution;
import org.librairy.modeler.lda.models.topic.TopicData;
import org.librairy.modeler.lda.models.topic.TopicDistribution;
import org.librairy.modeler.lda.models.topic.TopicModel;
import org.librairy.storage.UDM;
import org.librairy.storage.generator.URIGenerator;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;

/**
 * Created by cbadenes on 11/01/16.
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
public class TopicModelBuilderTest {

    private static final Logger LOG = LoggerFactory.getLogger(TopicModelBuilderTest.class);

    @Autowired
    TopicModelBuilder topicModelBuilder;

    @Autowired
    URIGenerator uriGenerator;


    @Autowired
    UDM udm;

    @Autowired
    RegularResourceBuilder regularResourceBuilder;

    @Test
    public void basic(){

        // Source
        Source source = Resource.newSource();
        source.setUri(uriGenerator.newFor(Resource.Type.SOURCE));
        udm.save(source);

        // Domain
        Domain domain = Resource.newDomain();
        domain.setUri(uriGenerator.newFor(Resource.Type.DOMAIN));
        domain.setName("test-domain");
        udm.save(domain);

        udm.save(Relation.newComposes(source.getUri(),domain.getUri()));
        
        // Analysis
        Analysis analysis = Resource.newAnalysis();
        analysis.setUri(uriGenerator.newFor(Resource.Type.ANALYSIS));
        analysis.setType("topic-model");
        analysis.setDomain(domain.getUri());
        analysis.setDescription("Topic Modeling using LDA");
        analysis.setConfiguration("");
        udm.save(analysis);

        // Document 1
        Document document1 = Resource.newDocument();
        document1.setUri(uriGenerator.newFor(Resource.Type.DOCUMENT));
        document1.setTitle("title-1");
        document1.setPublishedOn("20160112T12:07");
        udm.save(document1);
        udm.save(Relation.newProvides(source.getUri(),document1.getUri()));
        udm.save(Relation.newContains(domain.getUri(),document1.getUri()));


        // -> Item 1 from 1
        Item item11 = Resource.newItem();
        item11.setUri(uriGenerator.newFor(Resource.Type.ITEM));
        udm.save(item11);
        udm.save(Relation.newBundles(document1.getUri(),item11.getUri()));

        // -> Item 2 from 1
        Item item12 = Resource.newItem();
        item12.setUri(uriGenerator.newFor(Resource.Type.ITEM));
        udm.save(item12);
        udm.save(Relation.newBundles(document1.getUri(),item12.getUri()));

        // Document 2
        Document document2 = Resource.newDocument();
        document2.setUri(uriGenerator.newFor(Resource.Type.DOCUMENT));
        document2.setTitle("title-2");
        document2.setPublishedOn("20160112T12:07");
        udm.save(document2);

        udm.save(Relation.newProvides(source.getUri(),document2.getUri()));
        udm.save(Relation.newContains(domain.getUri(),document2.getUri()));


        // -> Item 1 from 2
        Item item21 = Resource.newItem();
        item21.setUri(uriGenerator.newFor(Resource.Type.ITEM));
        udm.save(item21);
        udm.save(Relation.newBundles(document2.getUri(),item21.getUri()));


        // -> Item 2 from 2
        Item item22 = Resource.newItem();
        item22.setUri(uriGenerator.newFor(Resource.Type.ITEM));
        udm.save(item22);
        udm.save(Relation.newBundles(document2.getUri(),item22.getUri()));


        // Creators
        User user = new User();
        user.setUri("http://librairy.org/users/833a8399-29b5-4e75-9118-d434d9f0273e");
        List<User> creators = Arrays.asList(new User[]{user});


        List<RegularResource> rrs = new ArrayList<>();
        rrs.add(regularResourceBuilder.from(item11.getUri(),document1.getTitle(), document1.getPublishedOn(), creators, "house place forest"));
        rrs.add(regularResourceBuilder.from(item12.getUri(),document1.getTitle(), document1.getPublishedOn(), creators, "home joy forest"));
        rrs.add(regularResourceBuilder.from(item21.getUri(),document2.getTitle(), document2.getPublishedOn(), creators, "house person forest"));
        rrs.add(regularResourceBuilder.from(item22.getUri(),document2.getTitle(), document2.getPublishedOn(), creators, "house play home"));

        TopicModel model = topicModelBuilder.build(domain.getUri(), rrs);


        LOG.info("Configuration: " + model.getConfiguration());


        Map<String,String> topicTable = new HashMap<>();
        for (TopicData topicData : model.getTopics()){

            // Save Topic
            Topic topic = new Topic();
            topic.setUri(uriGenerator.newFor(Resource.Type.TOPIC));
            topic.setAnalysis(analysis.getUri());
            topic.setContent("content");
            udm.save(topic);
            udm.save(Relation.newEmergesIn(topic.getUri(),domain.getUri()));

            topicTable.put(topicData.getId(),topic.getUri());

            // Relate it to Words
            for (WordDistribution wordDistribution : topicData.getWords()){

                List<String> result = udm.find(Resource.Type.WORD).by(Word.CONTENT, wordDistribution.getWord());
                String wordURI;
                if (result != null && !result.isEmpty()){
                    wordURI = result.get(0);
                }else {
                    wordURI = uriGenerator.newFor(Resource.Type.WORD);

                    // Create Word
                    Word word = new Word();
                    word.setUri(wordURI);
                    word.setContent(wordDistribution.getWord());
                    udm.save(word);

                }

                // Relate Topic to Word (mentions)
                MentionsFromTopic mention = Relation.newMentionsFromTopic(topic.getUri(), wordURI);
                mention.setWeight(wordDistribution.getWeight());
                udm.save(mention);

            }
        }


        Set<String> resourceURIs = model.getResources().keySet();
        for (String resourceURI: resourceURIs){

            for (TopicDistribution topicDistribution: model.getResources().get(resourceURI)){
                // Relate resource (Item) to Topic
                String topicURI = topicTable.get(topicDistribution.getTopic());

                DealsWithFromItem deals = Relation.newDealsWithFromItem(resourceURI, topicURI);
                deals.setWeight(topicDistribution.getWeight());
                udm.save(deals);

            }
        }
        LOG.info("Model built and saved: " + model);

    }

}
