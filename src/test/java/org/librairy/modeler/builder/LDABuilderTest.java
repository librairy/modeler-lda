/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.builder;

import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.CorpusBuilder;
import org.librairy.modeler.lda.builder.LDABuilder;
import org.librairy.modeler.lda.builder.TopicsBuilder;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.Text;
import org.librairy.modeler.lda.models.TopicDescription;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleBinaryOperator;
import java.util.stream.Collectors;

/**
 * Created on 27/06/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
//@TestPropertySource(properties = {
//        "librairy.columndb.host = wiener.dia.fi.upm.es",
//        "librairy.columndb.port = 5011",
//        "librairy.documentdb.host = wiener.dia.fi.upm.es",
//        "librairy.documentdb.port = 5021",
//        "librairy.graphdb.host = wiener.dia.fi.upm.es",
//        "librairy.graphdb.port = 5030",
//        "librairy.lda.maxiterations = 3",
//        "librairy.lda.vocabulary.size = 10000"
//})
public class LDABuilderTest {


    private static final Logger LOG = LoggerFactory.getLogger(LDABuilderTest.class);

    @Autowired
    LDABuilder ldaBuilder;

    @Autowired
    CorpusBuilder corpusBuilder;

    @Autowired
    TopicsBuilder topicsBuilder;

    @Autowired
    URIGenerator uriGenerator;

    @Autowired
    ModelingHelper helper;


    String domainURI = "http://librairy.org/domains/default";

    @Test
    public void buildByDomain(){

        Corpus corpus = corpusBuilder.build(domainURI, Arrays.asList(new Resource.Type[]{Resource.Type.ITEM}));

        TopicModel topicModel = ldaBuilder.build(corpus);

        LOG.info("Topic Model built: " + topicModel);

    }

//    @Test
//    public void buildByUris(){
//
//        List<String> uris = Arrays.asList(new String[]{
//                "http://drinventor.eu/items/9c8b49fbc507cfe9903fc9f08dc2a8c8",
//                "http://drinventor.eu/items/ec934613dfb9acddd68f89c579f24aff",
//                "http://drinventor.eu/items/7da1c096e73093f6404cd28946d313a6",
//                "http://drinventor.eu/items/71b295f128318469a3f04d25ae8b18c",
//                "http://drinventor.eu/items/cc9ca0ab0fe51b6ec99c09a2cba75249",
//                "http://drinventor.eu/items/2073b01f3c011e62903d27f8549167a8",
//                "http://drinventor.eu/items/6cead06fa9b7ec59daf55d186f1085b3",
//                "http://drinventor.eu/items/2906920d70fb1fc1a67d171245160e02",
//                "http://drinventor.eu/items/548b6e832a232c101f8d9ab3a16d6d95",
//                "http://drinventor.eu/items/d75ddc6e097d193a5cfee8396aab8e21"
//        });
//
//        Corpus corpus = corpusBuilder.build("test",uris);
//
//        ldaBuilder.build(corpus);
//
//    }

    @Test
    public void load(){

        String domainId = URIGenerator.retrieveId(domainURI);

        TopicModel topicModel = ldaBuilder.load(domainId);

        LOG.info("Vocabulary: " + topicModel.getVocabModel().vocabulary());
    }

    @Test
    public void inference(){

        String domainId = URIGenerator.retrieveId(domainURI);

        TopicModel topicModel = ldaBuilder.load(domainId);

        Corpus corpus = new Corpus("test", Arrays.asList(new Resource.Type[]{Resource.Type.ANY}), helper);

        // Load Text
        Text text = new Text();
        text.setId("t1");
        text.setContent("In 1968, Arthur Appel described the first algorithm for what would eventually become known as ray " +
                "casting - a basis point for almost all of modern 3D graphics, as well as the later pursuit of " +
                "photorealism in graphics.");
        corpus.loadTexts(Arrays.asList(new Text[]{text}));


        corpus.setCountVectorizerModel(topicModel.getVocabModel());


        // Documents
        RDD<Tuple2<Object, Vector>> documents = corpus.getBagOfWords();

        // LDA Model
        LocalLDAModel localLDAModel = topicModel.getLdaModel();

        // Topics distribution for documents
        Map<Long, String> documentsUri = corpus.getRegistry();

        // Topics distribution
        List<Tuple2<Object, Vector>> topicsDistributionArray = localLDAModel.topicDistributions(documents).toJavaRDD().collect();



        List<TopicDescription> topics = topicsBuilder.retrieve(topicModel).stream().sorted(new Comparator<TopicDescription>() {
            @Override
            public int compare(TopicDescription o1, TopicDescription o2) {
                return Integer.valueOf(o1.getId()).compareTo(Integer.valueOf(o2.getId()));
            }
        }).collect(Collectors.toList());

        topicsDistributionArray.forEach( td ->{

            String docId = corpus.getRegistry().get(td._1);
            LOG.info("Topics Distribution of Document: " + docId);


            double[] topicsWeights = td._2.toArray();
            for (int i=0; i< topicsWeights.length; i++){
                int finalI = i;

                TopicDescription topic = topics.get(i);
                String topicUri = uriGenerator.basedOnContent(Resource.Type.TOPIC,topic.getContent());


                Double weight = topicsWeights[i];

                LOG.info(" ["+topic.getId()+"] "+ topicUri + ":\t"+weight);
            }

            double sum = Arrays.stream(topicsWeights).reduce(new DoubleBinaryOperator() {
                @Override
                public double applyAsDouble(double left, double right) {
                    return left+right;
                }
            }).getAsDouble();
            LOG.info("Total: " + sum);
        });


    }

}
