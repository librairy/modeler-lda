package org.librairy.modeler.lda.builder;

import es.upm.oeg.epnoi.matching.metrics.similarity.JensenShannonSimilarity;
import lombok.Setter;
import org.apache.spark.api.java.JavaRDD;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.relations.Relationship;
import org.librairy.model.domain.relations.SimilarTo;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.helper.SparkHelper;
import org.librairy.storage.UDM;
import org.librairy.storage.generator.URIGenerator;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class SimilarityBuilder {

    private static Logger LOG = LoggerFactory.getLogger(SimilarityBuilder.class);

    @Autowired @Setter
    UDM udm;

    @Autowired @Setter
    UnifiedColumnRepository columnRepository;

    @Autowired @Setter
    URIGenerator uriGenerator;

    @Autowired @Setter
    SparkHelper sparkHelper;

    @Value("${spark.filesystem}") @Setter
    String fileSystemEndpoint;

    @Value("${librairy.modeler.maxiterations}") @Setter
    Integer maxIterations;

    @Value("${librairy.modeler.folder}")
    String modelFolder;

    @Value("${librairy.vocabulary.folder}")
    String vocabularyFolder;


    public void update(String domainUri){

        // Clean Similarities
        deleteExistingSimilarities(Relation.Type.SIMILAR_TO_DOCUMENTS, domainUri);
        deleteExistingSimilarities(Relation.Type.SIMILAR_TO_ITEMS, domainUri);
        deleteExistingSimilarities(Relation.Type.SIMILAR_TO_PARTS, domainUri);

        // Document Similarities
        // inferred from Item similarity ( see SimilarToItemEventHandler)

        // Items Similarities
        calculateSimilaritiesBetween(Resource.Type.ITEM,domainUri);

        // Parts Similarities
        calculateSimilaritiesBetween(Resource.Type.PART,domainUri);

    }

    private void deleteExistingSimilarities(Relation.Type type, String domainUri){
        LOG.debug("deleting existing " + type + " relations ..");
        udm.find(type).from(Resource.Type.DOMAIN, domainUri)
                .parallelStream()
                .forEach(relation -> udm.delete(type).byUri(relation.getUri()));
    }


    private void calculateSimilaritiesBetween(Resource.Type type, String domainUri){

        LOG.info("Calculating similarities similarityBetween "+type+" in domain: " + domainUri);
        List<Resource> resources = udm.find(type).from(Resource.Type.DOMAIN, domainUri);

        if (resources.isEmpty()) return;

        JavaRDD<Resource> resourcesRDD = sparkHelper.getSc().parallelize(resources);

        List<Tuple2<Resource, Resource>> pairs = resourcesRDD.cartesian(resourcesRDD)
                .filter(x -> x._1().getUri().compareTo(x._2().getUri()) > 0)
                .collect();

        LOG.info("Calculating similarities...");

        Relation.Type relType = dealsFrom(type);

        pairs.parallelStream().forEach( pair -> {

            List<Relationship> p1 = udm.find(relType)
                    .from(type, pair._1.getUri())
                    .stream()
                    .map(rel -> new Relationship(rel.getEndUri(), rel.getWeight()))
                    .collect(Collectors.toList());
            List<Relationship> p2 = udm.find(relType)
                    .from(type, pair._2.getUri())
                    .stream()
                    .map(rel -> new Relationship(rel.getEndUri(), rel.getWeight()))
                    .collect(Collectors.toList());

            Double similarity = similarityBetween(p1, p2);

            LOG.info("Attaching SIMILAR_TO in "+ type + " based on " + pair);
            SimilarTo simRel1 = newSimilarTo(type,pair._1.getUri(),pair._2.getUri(), domainUri);
            simRel1.setWeight(similarity);
            simRel1.setDomain(domainUri);
            udm.save(simRel1);
        });
    }

    public Double similarityBetween(List<Relationship> relationships1, List<Relationship> relationships2){

        if (relationships1.isEmpty() || relationships2.isEmpty()) return 0.0;

        Comparator<Relationship> byUri = (e1, e2) ->e1.getUri().compareTo(e2.getUri());

        double[] weights1 = relationships1.stream().sorted(byUri).mapToDouble(x -> x.getWeight()).toArray();
        double[] weights2 = relationships2.stream().sorted(byUri).mapToDouble(x -> x.getWeight()).toArray();

        LOG.debug("weight1: " + Arrays.toString(weights1) + " - weights2:" + Arrays.toString(weights2));

        return JensenShannonSimilarity.apply(weights1, weights2);
    }

    private Relation.Type dealsFrom(Resource.Type type){
        switch (type){
            case ITEM: return Relation.Type.DEALS_WITH_FROM_ITEM;
            case DOCUMENT: return Relation.Type.DEALS_WITH_FROM_DOCUMENT;
            case PART: return Relation.Type.DEALS_WITH_FROM_PART;
            default: throw new RuntimeException("Type : " + type + " not handled to calculate similarities");
        }
    }

    private SimilarTo newSimilarTo(Resource.Type type, String uri1, String uri2, String uri3){
        switch (type){
            case ITEM: return Relation.newSimilarToItems(uri1, uri2, uri3);
            case DOCUMENT: return Relation.newSimilarToDocuments(uri1, uri2, uri3);
            case PART: return Relation.newSimilarToParts(uri1, uri2, uri3);
            default: throw new RuntimeException("Type : " + type + " not handled to calculate similarities");
        }
    }

}
