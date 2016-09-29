/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.builder;

import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.librairy.model.domain.relations.EmbeddedIn;
import org.librairy.model.domain.relations.EmergesIn;
import org.librairy.model.domain.relations.MentionsFromTopic;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Resource;
import org.librairy.model.domain.resources.Topic;
import org.librairy.model.domain.resources.Word;
import org.librairy.modeler.lda.models.TopicDescription;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.storage.UDM;
import org.librairy.storage.executor.ParallelExecutor;
import org.librairy.storage.generator.URIGenerator;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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

    @Autowired
    UnifiedColumnRepository columnRepository;

    public Map<String,String> persist(TopicModel model){

        String domainURI = uriGenerator.from(Resource.Type.DOMAIN, model.getId());

        // Reading topics
        List<TopicDescription> topics = retrieve(model);

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

        if (topics.isEmpty()) return new ConcurrentHashMap<>();

        ConcurrentHashMap<String,String> topicRegistry = new ConcurrentHashMap<>();

        ParallelExecutor executor = new ParallelExecutor();
        for (TopicDescription topicDescription : topics){
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    String content = topicDescription.getContent();

                    // Save topic
                    Topic topic = Resource.newTopic(content);
                    topic.setUri(uriGenerator.basedOnContent(Resource.Type.TOPIC,topic.getContent()));
                    udm.save(topic);
                    topicRegistry.put(topicDescription.getId(),topic.getUri());

                    // Relate to Domain
                    EmergesIn emerges = Relation.newEmergesIn(topic.getUri(), domainUri);
                    emerges.setWeight(1.0);
                    udm.save(emerges);

                    // Save words
                    topicDescription.getWords().forEach(wordDescription -> {

                        Word word = Resource.newWord(wordDescription.getWord());
                        word.setUri(uriGenerator.basedOnContent(Resource.Type.WORD,word.getContent()));
                        udm.save(word); // save or update

                        // Relate to Domain
                        EmbeddedIn embeddedIn = Relation.newEmbeddedIn(word.getUri(), domainUri);
                        udm.save(embeddedIn);
                        // bug in Neo4j, not unique relations
                        udm.fixDuplicates(embeddedIn.getUri());

                        // Relate to Topic
                        MentionsFromTopic mention = Relation.newMentionsFromTopic(topic.getUri(), word.getUri());
                        mention.setWeight(wordDescription.getWeight());
                        udm.save(mention);


                    });
                    LOG.info("new topic created with uri: " + topic.getUri());
                }
            });
        }
        executor.awaitTermination(5, TimeUnit.MINUTES);
        LOG.info(topics.size() + " topics have been saved");
        return topicRegistry;
    }


    public Map<String,String> composeRegistry(TopicModel model){

        ConcurrentHashMap<String,String> topicRegistry = new ConcurrentHashMap<>();

        List<TopicDescription> topics = retrieve(model);

        topics.parallelStream().forEach(topicDescription -> {

            String content  = topicDescription.getContent();
            String uri      = uriGenerator.basedOnContent(Resource.Type.TOPIC,content);
            topicRegistry.put(topicDescription.getId(),uri);
        });
        return topicRegistry;
    }

    public void delete(String domainUri){

        Iterable<Relation> rels = columnRepository.findBy(Relation.Type.EMERGES_IN,"domain",domainUri);

        if (rels.iterator().hasNext()){
            LOG.info("Deleting previous topics in domain: " + domainUri);

            ParallelExecutor pExecutor = new ParallelExecutor();
            for (Relation rel: rels){

                pExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        String topicUri = rel.getStartUri();

                        LOG.info("Deleting existing topic: " + topicUri);


                        clean(Relation.Type.MENTIONS_FROM_TOPIC, topicUri);
//                        columnRepository.findBy(Relation.Type.MENTIONS_FROM_TOPIC,"topic",topicUri)
//                                .forEach(mention -> udm.delete(Relation.Type.MENTIONS_FROM_TOPIC).byUri(mention.getUri()));

                        clean(Relation.Type.DEALS_WITH_FROM_DOCUMENT, topicUri);
//                        columnRepository.findBy(Relation.Type.DEALS_WITH_FROM_DOCUMENT,"topic",topicUri)
//                                .forEach(deals -> udm.delete(Relation.Type.DEALS_WITH_FROM_DOCUMENT).byUri(deals.getUri()));

                        clean(Relation.Type.DEALS_WITH_FROM_ITEM, topicUri);
//                        columnRepository.findBy(Relation.Type.DEALS_WITH_FROM_ITEM,"topic",topicUri)
//                                .forEach(deals -> udm.delete(Relation.Type.DEALS_WITH_FROM_ITEM).byUri(deals.getUri()));

                        clean(Relation.Type.DEALS_WITH_FROM_PART, topicUri);
//                        columnRepository.findBy(Relation.Type.DEALS_WITH_FROM_PART,"topic",topicUri)
//                                .forEach(deals -> udm.delete(Relation.Type.DEALS_WITH_FROM_PART).byUri(deals.getUri()));

                        LOG.debug("Deleting EMERGES_IN from: " + topicUri);
                        udm.delete(Relation.Type.EMERGES_IN).byUri(rel.getUri());
                    }
                });
            }
            pExecutor.awaitTermination(30, TimeUnit.MINUTES);

            // Topics deleted in this thread to avoid deadlock
            Iterator<Relation> it = rels.iterator();
            while(it.hasNext()){
                String topicUri = it.next().getStartUri();
                udm.delete(Resource.Type.TOPIC).byUri(topicUri);
            }
            LOG.info("Topics deleted");
        }
    }

    private void clean(Relation.Type relType, String uri){
        LOG.debug("Deleting " + relType.route() + " from: " + uri);
        Iterable<Relation> list = columnRepository.findBy(relType,"topic",uri);
        if (list.iterator().hasNext()) {
            columnRepository.delete(relType,list);
        }
    }


}
