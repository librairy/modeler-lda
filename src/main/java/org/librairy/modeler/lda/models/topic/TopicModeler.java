package org.librairy.modeler.lda.models.topic;

import es.upm.oeg.epnoi.matching.metrics.domain.entity.RegularResource;
import org.librairy.model.domain.relations.EmergesIn;
import org.librairy.model.domain.relations.MentionsFromTopic;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.*;
import org.librairy.model.utils.TimeUtils;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.scheduler.ModelingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by cbadenes on 11/01/16.
 */
public class TopicModeler extends ModelingTask {

    private static final Logger LOG = LoggerFactory.getLogger(TopicModeler.class);

    private final ModelingHelper helper;

    private final String domainUri;

    private final Resource.Type resourceType;


    public TopicModeler(String domainUri, ModelingHelper modelingHelper, Resource.Type resourceType) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
        this.resourceType = resourceType;
    }


    @Override
    public void run() {

        LOG.info("ready to create a new topic model for domain: " + domainUri);

        // Delete previous Topics
        // TODO Improve the filter expression
        helper.getUdm().find(Resource.Type.TOPIC).from(Resource.Type.DOMAIN,domainUri).stream().
                filter(topic -> !helper.getUdm().find(resourceType).from(Resource.Type.TOPIC,topic).isEmpty() ).
                forEach(topic -> helper.getUdm().delete(Resource.Type.TOPIC).byUri(topic));

        // Build the model
        buildModelfor(resourceType);

    }


    private void buildModelfor(Resource.Type resourceType){
        try{
            LOG.info("Building a topic model for " + resourceType.name() + "s in domain: " + domainUri);

            List<RegularResource> regularResources = new ArrayList<>();

            switch(resourceType){
                //TODO Optimize using Spark.parallel
                case DOCUMENT: regularResources = helper.getUdm().find(Resource.Type.DOCUMENT).from(Resource.Type.DOMAIN, domainUri)
                        .stream().
                        map(uri -> helper.getUdm().read(Resource.Type.DOCUMENT).byUri(uri)).
                        filter(res -> res.isPresent()).map(res -> (Document) res.get()).
                        map(document -> helper.getRegularResourceBuilder().from(document.getUri(), document.getTitle(), document.getAuthoredOn(), helper.getAuthorBuilder().composeFromMetadata(document.getAuthoredBy()), document.getTokens())).
                        collect(Collectors.toList());
                    break;
                //TODO Optimize using Spark.parallel
                case ITEM: regularResources = helper.getUdm().find(Resource.Type.ITEM).from(Resource.Type.DOMAIN,domainUri)
                        .stream().
                        map(uri -> helper.getUdm().read(Resource.Type.ITEM).byUri(uri)).
                        filter(res -> res.isPresent()).map(res -> (Item) res.get()).
                        map(item -> helper.getRegularResourceBuilder().from(item.getUri(), item.getTitle(), item.getAuthoredOn(), helper.getAuthorBuilder().composeFromMetadata(item.getAuthoredBy()), item.getTokens())).
                        collect(Collectors.toList());
                    break;
                //TODO Optimize using Spark.parallel
                case PART: regularResources = helper.getUdm().find(Resource.Type.PART).from(Resource.Type.DOMAIN,domainUri)
                        .stream().
                        map(uri -> helper.getUdm().read(Resource.Type.PART).byUri(uri)).
                        filter(res -> res.isPresent()).map(res -> (Part) res.get()).
                        // TODO Improve metainformation of Part
                                map(part -> helper.getRegularResourceBuilder().from(part.getUri(), part.getSense(), part.getCreationTime(), new ArrayList<User>(), part.getTokens())).
                                collect(Collectors.toList());
                    break;
            }

            if ((regularResources == null) || (regularResources.isEmpty()))
                throw new RuntimeException("No " + resourceType.name() + "s found in domain: " + domainUri);

            // Create the analysis
            Analysis analysis = newAnalysis("Topic-Model","LDA with Evolutionary Algorithm parameterization",resourceType.name(),domainUri);

            // Persist Topic and Relations
            TopicModel model = helper.getTopicModelBuilder().build(domainUri, regularResources);
            persistModel(analysis,model,resourceType);

            // Save the analysis
            analysis.setConfiguration(model.getConfiguration().toString());
            helper.getUdm().save(analysis);
        } catch (RuntimeException e){
            LOG.warn(e.getMessage(),e);
        } catch (Exception e){
            LOG.error(e.getMessage(),e);
        }
    }

    private void persistModel(Analysis analysis, TopicModel model, Resource.Type resourceType){
        Map<String,String> topicTable = new HashMap<>();
        for (TopicData topicData : model.getTopics()){

            // Save Topic
            Topic topic = Resource.newTopic();
            topic.setAnalysis(analysis.getUri());
            topic.setContent(String.join(",",topicData.getWords().stream().map(wd -> wd.getWord()).collect(Collectors.toList())));
            topic.setUri(helper.getUriGenerator().basedOnContent(Resource.Type.TOPIC,topic.getContent()));
            helper.getUdm().save(topic);

            EmergesIn emerges = Relation.newEmergesIn(topic.getUri(), domainUri);
            emerges.setAnalysis(analysis.getUri());
            helper.getUdm().save(emerges);


            topicTable.put(topicData.getId(),topic.getUri());

            ConcurrentHashMap<String,String> words = new ConcurrentHashMap<>();

            // Relate it to Words
            // TODO parallelStream does not work with graph-db
            topicData.getWords().stream().forEach( wordDistribution -> {

                if (!words.contains(wordDistribution.getWord())){
                    List<String> result = helper.getUdm().find(Resource.Type.WORD).by(Word.CONTENT, wordDistribution.getWord());
                    String wordURI;
                    if (result != null && !result.isEmpty()){
                        wordURI = result.get(0);
                    }else {
                        wordURI = helper.getUriGenerator().basedOnContent(Resource.Type.WORD,wordDistribution.getWord());

                        // Create Word
                        Word word = Resource.newWord();
                        word.setUri(wordURI);
                        word.setCreationTime(TimeUtils.asISO());
                        word.setContent(wordDistribution.getWord());
                        helper.getUdm().save(word);

                    }
                    words.put(wordDistribution.getWord(),wordURI);
                }

                String wordURI = words.get(wordDistribution.getWord());

                // Relate Topic to Word (mentions)
                MentionsFromTopic mentions = Relation.newMentionsFromTopic(topic.getUri(), wordURI);
                mentions.setWeight(wordDistribution.getWeight());
                helper.getUdm().save(mentions);
            });
        }

        // TODO parallelStream does not work with graph-db
        model.getResources().keySet().stream().forEach(resourceURI ->{
            for (TopicDistribution topicDistribution: model.getResources().get(resourceURI)){
                // Relate resource  to Topic
                String topicURI = topicTable.get(topicDistribution.getTopic());
                Relation relation = null;
                switch(resourceType){
                    case DOCUMENT:
                        relation = Relation.newDealsWithFromDocument(resourceURI,topicURI);
                        break;
                    case ITEM:
                        relation = Relation.newDealsWithFromItem(resourceURI,topicURI);
                        break;
                    case PART:
                        relation = Relation.newDealsWithFromPart(resourceURI,topicURI);
                        break;
                }
                relation.setWeight(topicDistribution.getWeight());
                helper.getUdm().save(relation);
            }
        });

        LOG.info("Topic Model saved in ddbb: " + model);
    }
}
