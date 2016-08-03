package org.librairy.modeler.builder;

import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.relations.Relationship;
import org.librairy.model.domain.relations.SimilarTo;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.OnlineLDABuilder;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.similarity.RelationalSimilarity;
import org.librairy.modeler.lda.models.topic.DensityDistribution;
import org.librairy.modeler.lda.models.topic.WeightedPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created on 27/06/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
        "librairy.modeler.learn = false",
        "librairy.comparator.delay = 1000",
        "librairy.cassandra.contactpoints = wiener.dia.fi.upm.es",
        "librairy.cassandra.port = 5011",
        "librairy.cassandra.keyspace = research",
        "librairy.elasticsearch.contactpoints = wiener.dia.fi.upm.es",
        "librairy.elasticsearch.port = 5021",
        "librairy.neo4j.contactpoints = wiener.dia.fi.upm.es",
        "librairy.neo4j.port = 5030",
        "librairy.eventbus.host = wiener.dia.fi.upm.es",
        "librairy.eventbus.port = 5041",
})
public class SimilarityBuilderTest {


    private static final Logger LOG = LoggerFactory.getLogger(SimilarityBuilderTest.class);

    @Autowired
    OnlineLDABuilder builder;

    @Autowired
    ModelingHelper modelingHelper;

    @Test
    public void calculateSimilaritiesTest(){

        String domainUri    = "http://drinventor.eu/domains/4f56ab24bb6d815a48b8968a3b157470";

        // Clean Similatities
//        LOG.debug("deleting existing similarities ..");
//        modelingHelper.getUdm().find(Relation.Type.SIMILAR_TO_DOCUMENTS).from(Resource.Type.DOMAIN, domainUri).parallelStream().forEach(relation
//                -> {
//            LOG.debug("Deleting relation SIMILAR_TO_DOCS: " + relation.getUri());
//            modelingHelper.getUdm().delete(Relation.Type.SIMILAR_TO_DOCUMENTS).byUri(relation.getUri());
//        });
//
//        modelingHelper.getUdm().find(Relation.Type.SIMILAR_TO_ITEMS).from(Resource.Type.DOMAIN, domainUri).parallelStream().forEach(relation
//                -> {
//            LOG.debug("Deleting relation SIMILAR_TO_ITEMS: " + relation.getUri());
//            modelingHelper.getUdm().delete(Relation.Type.SIMILAR_TO_ITEMS).byUri(relation.getUri());
//        });
//
//        modelingHelper.getUdm().find(Relation.Type.SIMILAR_TO_PARTS).from(Resource.Type.DOMAIN, domainUri).parallelStream().forEach(relation
//                -> {
//            LOG.debug("Deleting relation SIMILAR_TO_PARTS: " + relation.getUri());
//            modelingHelper.getUdm().delete(Relation.Type.SIMILAR_TO_PARTS).byUri(relation.getUri());
//        });

        // Items Similarities
        LOG.info("Calculating similarities between parts in domain: " + domainUri);
        calculateSimilaritiesBetweenParts(domainUri);

    }


    private void calculateSimilaritiesBetweenItems(String domainUri){

        LOG.info("Reading items from domain: " + domainUri);
        List<String> items = modelingHelper.getUdm().find(Resource.Type.ITEM).from(Resource.Type.DOMAIN, domainUri);

        JavaRDD<String> itemsRDD = modelingHelper.getSparkHelper().getSc().parallelize(items);

        List<Tuple2<String, String>> itemsPair = itemsRDD.cartesian(itemsRDD)
                .filter(x -> x._1().compareTo(x._2()) > 0)
                .collect();

        LOG.info("Calculating similarities...");
        itemsPair.parallelStream().forEach( pair -> {

            List<Relationship> p1 = modelingHelper.getUdm().find(Relation.Type.DEALS_WITH_FROM_ITEM).from(Resource.Type
                    .ITEM, pair._1).stream().map(rel -> new Relationship(rel.getEndUri(), rel.getWeight())).collect
                    (Collectors.toList());
            List<Relationship> p2 = modelingHelper.getUdm().find(Relation.Type.DEALS_WITH_FROM_ITEM).from(Resource.Type
                    .ITEM, pair._2).stream().map(rel -> new Relationship(rel.getEndUri(), rel.getWeight())).collect
                    (Collectors.toList());

            Double similarity = RelationalSimilarity.between(p1, p2);

            LOG.info("Attaching SIMILAR_TO based on " + pair);
            SimilarTo simRel1 = Relation.newSimilarToItems(pair._1, pair._2);
            SimilarTo simRel2 = Relation.newSimilarToItems(pair._2, pair._1);

            simRel1.setWeight(similarity);
            simRel1.setDomain(domainUri);
            modelingHelper.getUdm().save(simRel1);

            simRel2.setWeight(similarity);
            simRel2.setDomain(domainUri);
            modelingHelper.getUdm().save(simRel2);

        });

    }


    private void calculateSimilaritiesBetweenParts(String domainUri){

        LOG.info("Reading parts from domain: " + domainUri);
        List<String> parts = modelingHelper.getUdm().find(Resource.Type.PART).from(Resource.Type.DOMAIN, domainUri);

        JavaRDD<String> urisRDD = modelingHelper.getSparkHelper().getSc().parallelize(parts);

        List<Tuple2<String, String>> pairs = urisRDD.cartesian(urisRDD)
                .filter(x -> x._1().compareTo(x._2()) > 0)
                .collect();

        LOG.info("Calculating similarities...");
        pairs.parallelStream().forEach( pair -> {

            List<Relationship> p1 = modelingHelper.getUdm().find(Relation.Type.DEALS_WITH_FROM_PART).from(Resource.Type
                    .PART, pair._1).stream().map(rel -> new Relationship(rel.getEndUri(), rel.getWeight())).collect
                    (Collectors.toList());
            List<Relationship> p2 = modelingHelper.getUdm().find(Relation.Type.DEALS_WITH_FROM_PART).from(Resource.Type
                    .PART, pair._2).stream().map(rel -> new Relationship(rel.getEndUri(), rel.getWeight())).collect
                    (Collectors.toList());

            Double similarity = RelationalSimilarity.between(p1, p2);

            LOG.info("Attaching SIMILAR_TO (PART) based on " + pair);
            SimilarTo simRel1 = Relation.newSimilarToParts(pair._1, pair._2);
            SimilarTo simRel2 = Relation.newSimilarToParts(pair._2, pair._1);

            simRel1.setWeight(similarity);
            simRel1.setDomain(domainUri);
            modelingHelper.getUdm().save(simRel1);

            simRel2.setWeight(similarity);
            simRel2.setDomain(domainUri);
            modelingHelper.getUdm().save(simRel2);

        });

    }

}
