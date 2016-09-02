package org.librairy.modeler.lda.builder;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.*;
import org.librairy.storage.UDM;
import org.librairy.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class TopicsDistributionBuilder {

    private static Logger LOG = LoggerFactory.getLogger(TopicsDistributionBuilder.class);

    @Autowired
    UDM udm;

    @Autowired
    LDABuilder ldaBuilder;

    @Autowired
    URIGenerator uriGenerator;

    @Autowired
    ModelingHelper helper;

    @Autowired
    TopicsBuilder topicsBuilder;


    public List<TopicsDistribution> inference(String domainUri, List<Text> texts){

        String domainId = URIGenerator.retrieveId(domainUri);

        // Load existing model for domain
        TopicModel topicModel = ldaBuilder.load(domainId);

        // Create a minimal corpus
        Corpus corpus = new Corpus("from-inference",helper);
        corpus.loadTexts(texts);

        // Use vocabulary from existing model
        corpus.setCountVectorizerModel(topicModel.getVocabModel());

        // Documents
        RDD<Tuple2<Object, Vector>> documents = corpus.getBagOfWords().cache();


        // Topics Description
        Comparator<TopicDescription> numericalSorter = new Comparator<TopicDescription>() {
            @Override
            public int compare(TopicDescription o1, TopicDescription o2) {
                return Integer.valueOf(o1.getId()).compareTo(Integer.valueOf(o2.getId()));
            }
        };
        List<TopicDescription> topics = topicsBuilder.retrieve(topicModel)
                .stream()
                .sorted(numericalSorter)
                .collect(Collectors.toList());


        // Topics distribution
        return topicModel.getLdaModel().topicDistributions(documents)
                .toJavaRDD()
                .collect()
                .parallelStream()
                .map( td ->{

                    TopicsDistribution topicsDistribution = new TopicsDistribution();

                    topicsDistribution.setDocumentId(corpus.getRegistry().get(td._1));

                    double[] topicsWeights = td._2.toArray();
                    for (int i=0; i< topicsWeights.length; i++){
                        topicsDistribution.add(
                                uriGenerator.basedOnContent(Resource.Type.TOPIC,topics.get(i).getContent()),
                                topicsWeights[i]);
                    }

                    return topicsDistribution;
        }).collect(Collectors.toList());

    }


}
