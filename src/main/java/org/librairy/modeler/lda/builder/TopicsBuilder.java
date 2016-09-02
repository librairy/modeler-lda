package org.librairy.modeler.lda.builder;

import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.librairy.model.domain.relations.EmergesIn;
import org.librairy.model.domain.relations.MentionsFromTopic;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Resource;
import org.librairy.model.domain.resources.Topic;
import org.librairy.model.domain.resources.Word;
import org.librairy.modeler.lda.models.TopicDescription;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.storage.UDM;
import org.librairy.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class TopicsBuilder {

    private static Logger LOG = LoggerFactory.getLogger(TopicsBuilder.class);

    @Autowired
    UDM udm;

    @Autowired
    URIGenerator uriGenerator;

    @Value("#{environment['LIBRAIRY_LDA_WORDS_PER_TOPIC']?:${librairy.lda.topic.words}}")
    Integer maxWords;


    public Map<String,String> persist(TopicModel model){

        String domainURI = uriGenerator.from(Resource.Type.DOMAIN, model.getId());

        // Reading topics
        List<TopicDescription> topics = retrieve(model);

        // Deleting existing topics
        delete(domainURI);

        // Persist the new ones
        Map<String, String> topicRegistry = save(topics, domainURI);

        return topicRegistry;

    }


    public List<TopicDescription> retrieve(TopicModel model){

        List<TopicDescription> topics = new ArrayList<>();

        LocalLDAModel ldaModel = model.getLdaModel();

        Tuple2<int[], double[]>[] topicIndices = ldaModel.describeTopics(maxWords);
        String[] vocabulary = model.getVocabModel().vocabulary();

        int index = 0;
        for (Tuple2<int[], double[]> topicDistribution : topicIndices){

            LOG.info("Reading topic" + index);
            TopicDescription topicDescription = new TopicDescription(String.valueOf(index));

            int[] wordsId = topicDistribution._1;
            double[] weights = topicDistribution._2;

            for (int i=0; i< wordsId.length;i++){
                int wid = wordsId[i];
                String word     = vocabulary[wid];
                Double weight   = weights[i];
                topicDescription.add(word, weight);

            }

            topics.add(topicDescription);
            index++;
        }
        return topics;
    }

    public Map<String,String> save(List<TopicDescription> topics, String domainUri){

        ConcurrentHashMap<String,String> topicRegistry = new ConcurrentHashMap<>();

        topics.parallelStream().forEach(topicDescription -> {

            String content = topicDescription.getContent();

            // Save topic
            Topic topic = Resource.newTopic(content);
            topic.setUri(uriGenerator.basedOnContent(Resource.Type.TOPIC,topic.getContent()));
            LOG.info("Saving topic: [" + topicDescription.getId()+"]" + topic.getUri());
            udm.save(topic);
            topicRegistry.put(topicDescription.getId(),topic.getUri());

            // Relate to Domain
            EmergesIn emerges = Relation.newEmergesIn(topic.getUri(), domainUri);
            emerges.setWeight(1.0);
            udm.save(emerges);

            // Save words
            topicDescription.getWords().parallelStream().forEach(wordDescription -> {

                Word word = Resource.newWord(wordDescription.getWord());
                word.setUri(uriGenerator.basedOnContent(Resource.Type.WORD,word.getContent()));
                udm.save(word); // save or update

                // Relate to Topic
                MentionsFromTopic mention = Relation.newMentionsFromTopic(topic.getUri(), word.getUri());
                mention.setWeight(wordDescription.getWeight());
                udm.save(mention);

            });

        });
        return topicRegistry;
    }

    public void delete(String domainUri){


        // columnRepository.findBy(Relation.Type.EMERGES_IN, "domain", domainUri).forEach(relation -> {

        LOG.info("Deleting existing topics in domain: " + domainUri);
        udm.find(Resource.Type.TOPIC).from(Resource.Type.DOMAIN, domainUri).parallelStream().forEach(topic -> {
            LOG.info("Trying to delete topic: " + topic.getUri());

            LOG.info("Deleting MENTIONS from: " + topic.getUri());
            udm.find(Relation.Type.MENTIONS_FROM_TOPIC)
                    .from(Resource.Type.TOPIC, topic.getUri())
                    .parallelStream()
                    .forEach(rel ->
                            udm.delete(Relation.Type.MENTIONS_FROM_TOPIC).byUri(rel.getUri()));

            LOG.info("Deleting EMERGES_IN from: " + topic.getUri());
            udm.find(Relation.Type.EMERGES_IN)
                    .from(Resource.Type.TOPIC, topic.getUri())
                    .parallelStream()
                    .forEach(rel ->
                            udm.delete(Relation.Type.EMERGES_IN).byUri(rel.getUri()));

            LOG.info("Deleting DEALS_WITH_FROM_DOCUMENT from: " + topic.getUri());
            udm.find(Relation.Type.DEALS_WITH_FROM_DOCUMENT)
                    .from(Resource.Type.TOPIC, topic.getUri())
                    .parallelStream().forEach(rel ->
                    udm.delete(Relation.Type.DEALS_WITH_FROM_DOCUMENT).byUri(rel.getUri()));

            LOG.info("Deleting DEALS_WITH_FROM_ITEM from: " + topic.getUri());
            udm.find(Relation.Type.DEALS_WITH_FROM_ITEM)
                    .from(Resource.Type.TOPIC, topic.getUri())
                    .parallelStream().forEach(rel -> udm.delete(Relation.Type.DEALS_WITH_FROM_ITEM).byUri(rel
                    .getUri()));

            LOG.info("Deleting DEALS_WITH_FROM_PART from: " + topic.getUri());
            udm.find(Relation.Type.DEALS_WITH_FROM_PART)
                    .from(Resource.Type.TOPIC, topic.getUri())
                    .parallelStream().forEach(rel -> udm.delete(Relation.Type.DEALS_WITH_FROM_PART).byUri(rel.getUri
                    ()));

            LOG.info("Deleting TOPIC: " + topic.getUri());
            udm.delete(Resource.Type.TOPIC).byUri(topic.getUri());

        });
        LOG.info("Topics deleted");

    }


}
