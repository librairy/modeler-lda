package org.librairy.modeler.lda.models.topic;

import es.upm.oeg.epnoi.matching.metrics.domain.entity.RegularResource;
import org.apache.spark.api.java.JavaRDD;
import org.librairy.model.domain.relations.EmergesIn;
import org.librairy.model.domain.relations.MentionsFromTopic;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.relations.SimilarTo;
import org.librairy.model.domain.resources.*;
import org.librairy.model.utils.TimeUtils;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.similarity.RelationalSimilarity;
import org.librairy.modeler.lda.scheduler.ModelingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

        // Remove existing topics in domain
        clean();

        // Create a new Topic Model
        List<String> uris = build();

        // Calculate similarities based on the model
        calculateSimilarities();

    }

    public void clean(){
        // Delete previous Topics
        helper.getColumnRepository().findBy(Relation.Type.EMERGES_IN, "domain", domainUri).forEach(relation -> helper
                .getUdm().delete(Resource.Type.TOPIC).byUri(relation.getStartUri()));
//        helper.getUdm().find(Resource.Type.TOPIC).from(Resource.Type.DOMAIN,domainUri).stream().
//                filter(topic -> !helper.getUdm().find(resourceType).from(Resource.Type.TOPIC,topic).isEmpty() ).
//                forEach(topic -> helper.getUdm().delete(Resource.Type.TOPIC).byUri(topic));


    }

    private List<String> build(){
        List<String> uris = new ArrayList<>();
        try{
            LOG.info("Building a topic model for " + resourceType.name() + "s in domain: " + domainUri);

            uris = helper.getUdm().find(Resource.Type.ITEM).from(Resource.Type.DOMAIN, domainUri);

            List<RegularResource> regularResources = uris.parallelStream().
                            map(uri -> helper.getUdm().read(Resource.Type.ITEM).byUri(uri)).
                            filter(res -> res.isPresent()).map(res -> (Item) res.get()).
                            map(item -> helper.getRegularResourceBuilder().from(item.getUri(), item.getTitle(), item.getAuthoredOn(), helper.getAuthorBuilder().composeFromMetadata(item.getAuthoredBy()), item.getTokens())).
                            collect(Collectors.toList());

            if ((regularResources == null) || (regularResources.isEmpty()))
                throw new RuntimeException("No " + resourceType.name() + "s found in domain: " + domainUri);

            // Create the analysis
            Analysis analysis = newAnalysis("Topic-Model","LDA with Evolutionary Algorithm parametrization",resourceType.name(),domainUri);

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
        return uris;
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

            String itemUri = resourceURI;

            // DEALS_WITH (from ITEM)
            for (TopicDistribution topicDistribution: model.getResources().get(itemUri)){
                // Relate ITEM to Topic
                String topicURI = topicTable.get(topicDistribution.getTopic());

                Relation relation = Relation.newDealsWithFromItem(itemUri,topicURI);
                relation.setWeight(topicDistribution.getWeight());
                helper.getUdm().save(relation);
            }
        });

        LOG.info("Topic Model saved in ddbb: " + model);
    }


    private void calculateSimilarities(){
        LOG.debug("deleting existing similarities ..");
        // TODO BUG in Neo4j: this action will remove other relations
        helper.getUdm().delete(Relation.Type.SIMILAR_TO_ITEMS).in(Resource.Type.DOMAIN, domainUri);

        // Get topic distributions
        Iterable<Relation> relations = helper.getUdm().find(Relation.Type.DEALS_WITH_FROM_ITEM).from(Resource.Type.DOMAIN, domainUri);

        // Calculate Similarities
        List<WeightedPair> similarities = compute(StreamSupport.stream(relations.spliterator(), false).map(rel -> new WeightedPair(rel.getStartUri(), rel.getEndUri(), rel.getWeight())).collect(Collectors.toList()));

        // Save similarities in ddbb
        similarities.parallelStream().forEach(pair -> {

            LOG.info("Attaching SIMILAR_TO based on " + pair);
            SimilarTo simRel1 = Relation.newSimilarToItems(pair.getUri1(), pair.getUri2());
            SimilarTo simRel2 = Relation.newSimilarToItems(pair.getUri2(), pair.getUri1());

            simRel1.setWeight(pair.getWeight());
            simRel1.setDomain(domainUri);
            helper.getUdm().save(simRel1);

            simRel2.setWeight(pair.getWeight());
            simRel2.setDomain(domainUri);
            helper.getUdm().save(simRel2);
        });

    }

    protected List<WeightedPair> compute(List<WeightedPair> pairs){

        LOG.info("Computing SIMILAR_TO based on Topic Models..");

        JavaRDD<DensityDistribution> topicDistributions = helper.getSparkHelper().getSc().parallelize(pairs).
                mapToPair(x -> new Tuple2<String, WeightedPair>(x.getUri1(), x)).
                groupByKey().
                map(x -> new DensityDistribution(x._1(), x._2()))
                ;

        LOG.debug("Topic Distributions: "+topicDistributions.collect().size());

        List<WeightedPair> similarities = topicDistributions.
                cartesian(topicDistributions).
                filter(x -> x._1().getUri().compareTo(x._2().getUri()) > 0).
                filter(x -> x._1().getRelationships().size() == x._2().getRelationships().size()).
                map(x -> new WeightedPair(x._1().getUri(), x._2().getUri(), RelationalSimilarity.between(x._1().getRelationships(), x._2().getRelationships()))).
                collect();

        LOG.debug("Similarities: "+similarities);
        return similarities;

    }
}
