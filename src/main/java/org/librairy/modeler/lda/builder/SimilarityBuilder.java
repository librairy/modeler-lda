/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.builder;

import com.google.common.collect.ImmutableMap;
import lombok.Setter;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.spark.api.java.JavaRDD;
import org.librairy.computing.helper.SparkHelper;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.model.Event;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.relations.Relationship;
import org.librairy.model.domain.relations.SimilarTo;
import org.librairy.model.domain.relations.SimilarToItems;
import org.librairy.model.domain.resources.Resource;
import org.librairy.model.modules.EventBus;
import org.librairy.model.modules.RoutingKey;
import org.librairy.model.utils.ResourceUtils;
import org.librairy.modeler.lda.models.SimilarResource;
import org.librairy.modeler.lda.models.TopicDistribution;
import org.librairy.storage.UDM;
import org.librairy.storage.executor.ParallelExecutor;
import org.librairy.storage.generator.URIGenerator;
import org.librairy.storage.system.column.domain.SimilarToColumn;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.librairy.storage.system.column.templates.ColumnTemplate;
import org.librairy.storage.system.graph.cache.GraphCache;
import org.librairy.storage.system.graph.domain.edges.Edge;
import org.librairy.storage.system.graph.domain.nodes.Node;
import org.librairy.storage.system.graph.repository.edges.UnifiedEdgeGraphRepository;
import org.librairy.storage.system.graph.repository.edges.UnifiedEdgeGraphRepositoryFactory;
import org.librairy.storage.system.graph.template.TemplateExecutor;
import org.librairy.storage.system.graph.template.TemplateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    @Autowired
    ColumnTemplate columnTemplate;

    @Autowired
    TemplateFactory factory;

    @Autowired
    GraphCache cache;

    @Autowired
    EventBus eventBus;

    @Autowired @Setter
    URIGenerator uriGenerator;

    @Autowired @Setter
    SparkHelper sparkHelper;

    @Autowired
    TemplateExecutor graphExecutor;


    public void discover(String domainUri, Resource.Type type){

        LOG.info("Discovering similarities between "+type.route()+" in domain: " + domainUri +" ..");


        List<Resource> resources = udm.find(type).from(Resource.Type.DOMAIN, domainUri);

        if (resources.isEmpty()){
            LOG.info("No "+type.route()+" found in domain: " + domainUri);
            return;
        }
        JavaRDD<Resource> resourcesRDD = sparkHelper.getContext().parallelize(resources);

        List<Tuple2<Resource, Resource>> pairs = resourcesRDD.cartesian(resourcesRDD)
                .filter(x -> x._1().getUri().compareTo(x._2().getUri()) > 0)
                .collect();

        Relation.Type relType = dealsFrom(type);

        if (!pairs.isEmpty()){
            ParallelExecutor executor = new ParallelExecutor();

            for (Tuple2<Resource, Resource> pair: pairs){
                executor.execute(() -> {

                    List<Relationship> p1 = new ArrayList<Relationship>();
                    columnRepository.findBy(relType,pair._1.getResourceType().key(),pair._1.getUri()).forEach(rel
                            -> p1.add(new Relationship(rel.getEndUri(), rel.getWeight())));

                    List<Relationship> p2 = new ArrayList<Relationship>();
                    columnRepository.findBy(relType,pair._2.getResourceType().key(),pair._2.getUri()).forEach(rel
                            -> p2.add(new Relationship(rel.getEndUri(), rel.getWeight())));

                    Double similarity = similarityBetween(p1, p2);

                    LOG.debug("created SIMILAR_TO relation between " + pair);
                    SimilarTo simRel1 = newSimilarTo(type,pair._1.getUri(),pair._2.getUri(), domainUri);
                    simRel1.setWeight(similarity);
                    simRel1.setDomain(domainUri);
                    simRel1.setUri(uriGenerator.basedOnContent(type,pair._1.getUri()+""+pair._2.getUri()+""+domainUri));
//                    udm.save(simRel1);
                    columnRepository.save(simRel1);

                    // publish event
                    eventBus.post(Event.from(ResourceUtils.map(simRel1,Relation.class)), RoutingKey.of(simRel1.getType
                            (), Relation.State.CREATED));

                });
            }
            executor.awaitTermination(1, TimeUnit.HOURS);
        }
        LOG.info("persisting similarities on graph database..");
        Relation.Type simRelType = similarFrom(type);

        AtomicInteger counter = new AtomicInteger();
        columnRepository.findBy(simRelType, "domain", domainUri).forEach(rel-> {

            if (!URIGenerator.typeFrom(rel.getStartUri()).equals(type)) return;

            counter.incrementAndGet();
            SimilarToColumn columnRel = (SimilarToColumn) rel;
            Relation simRel = (Relation) ResourceUtils.map(columnRel, Relation.classOf(simRelType));
            factory.of(simRelType).save(simRel);
        });
        LOG.info(counter.get() + " similarities discovered!!");
    }

    public List<SimilarResource> topSimilars(Resource.Type type, String domainUri, Integer n,  List<TopicDistribution>
            topicsDistribution){

        LOG.info("Getting top "+n+" similar " + type.route() + " to a given one in domain: " + domainUri);

        List<Relationship> topicsInText = topicsDistribution.stream()
                .map(td -> new Relationship(td.getTopicUri(),td.getWeight()))
                .collect(Collectors.toList());

        Comparator<? super SimilarResource> byWeight = new Comparator<SimilarResource>() {
            @Override
            public int compare(SimilarResource o1, SimilarResource o2) {
                return -o1.getWeight().compareTo(o2.getWeight());
            }
        };

        return udm.find(type)
                .from(Resource.Type.DOMAIN, domainUri)
                .parallelStream()
                .map( resource -> {

                    List<Relationship> p1 = udm.find(dealsFrom(type))
                            .from(type, resource.getUri())
                            .stream()
                            .map(rel -> new Relationship(rel.getEndUri(), rel.getWeight()))
                            .collect(Collectors.toList());

                    Double similarity = similarityBetween(p1, topicsInText);

                    SimilarResource sr = new SimilarResource();
                    sr.setUri(resource.getUri());
                    sr.setWeight(similarity);
                    return sr;
                })
                .sorted(byWeight)
                .limit(n)
                .collect(Collectors.toList())
        ;
    }


    public void delete(String domainUri){

        LOG.info("Deleting previous similar-to relations from column-database...");
        columnRepository.findBy(Relation.Type.SIMILAR_TO_ITEMS, "domain", domainUri).forEach(rel-> {
            Relation.Type relType = similarFrom(URIGenerator.typeFrom(rel.getStartUri()));
            columnRepository.delete(relType,rel.getUri());
        });

        LOG.info("Deleting previous similar-to relations from graph-database...");
        graphExecutor.execute("match ()-[r:SIMILAR_TO { domain : {0} }]->() delete r", ImmutableMap.of("0",
                domainUri));
        LOG.info("similar-to relations deleted");
    }


    public Double similarityBetween(List<Relationship> relationships1, List<Relationship> relationships2){

        if ((relationships1.isEmpty() || relationships2.isEmpty())
                || ((relationships1.size() != relationships2.size()
        ))) return 0.0;

        Comparator<Relationship> byUri = (e1, e2) ->e1.getUri().compareTo(e2.getUri());

        double[] weights1 = relationships1.stream().sorted(byUri).mapToDouble(x -> x.getWeight()).toArray();
        double[] weights2 = relationships2.stream().sorted(byUri).mapToDouble(x -> x.getWeight()).toArray();

        LOG.debug("weight1: " + Arrays.toString(weights1) + " - weights2:" + Arrays.toString(weights2));

        return JensenShannonSimilarity.apply(weights1, weights2);
    }


    private Relation.Type similarFrom(Resource.Type type){
        switch (type){
            case ITEM: return Relation.Type.SIMILAR_TO_ITEMS;
            case DOCUMENT: return Relation.Type.SIMILAR_TO_DOCUMENTS;
            case PART: return Relation.Type.SIMILAR_TO_PARTS;
            default: throw new RuntimeException("Type : " + type + " not handled to discover similarities");
        }
    }

    private Relation.Type dealsFrom(Resource.Type type){
        switch (type){
            case ITEM: return Relation.Type.DEALS_WITH_FROM_ITEM;
            case DOCUMENT: return Relation.Type.DEALS_WITH_FROM_DOCUMENT;
            case PART: return Relation.Type.DEALS_WITH_FROM_PART;
            default: throw new RuntimeException("Type : " + type + " not handled to discover similarities");
        }
    }

    private SimilarTo newSimilarTo(Resource.Type type, String uri1, String uri2, String domainUri){
        switch (type){
            case ITEM: return Relation.newSimilarToItems(uri1, uri2, domainUri);
            case DOCUMENT: return Relation.newSimilarToDocuments(uri1, uri2, domainUri);
            case PART: return Relation.newSimilarToParts(uri1, uri2, domainUri);
            default: throw new RuntimeException("Type : " + type + " not handled to discover similarities");
        }
    }

}
