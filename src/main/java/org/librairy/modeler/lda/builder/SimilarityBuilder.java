/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.builder;

import com.google.common.collect.ImmutableMap;
import lombok.Setter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.librairy.computing.helper.SparkHelper;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.relations.Relationship;
import org.librairy.model.domain.relations.SimilarTo;
import org.librairy.model.domain.resources.Resource;
import org.librairy.model.modules.EventBus;
import org.librairy.modeler.lda.functions.RelationBuilder;
import org.librairy.modeler.lda.functions.SimilarityCalculator;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.SimilarResource;
import org.librairy.modeler.lda.models.TopicDistribution;
import org.librairy.storage.UDM;
import org.librairy.storage.executor.ParallelExecutor;
import org.librairy.storage.generator.URIGenerator;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.librairy.storage.system.column.templates.ColumnTemplate;
import org.librairy.storage.system.graph.cache.GraphCache;
import org.librairy.storage.system.graph.template.TemplateExecutor;
import org.librairy.storage.system.graph.template.TemplateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
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
    ModelingHelper helper;

    @Autowired
    TemplateExecutor graphExecutor;


    public void discover(String domainUri, Resource.Type type){

        LOG.info("Discovering similarities between "+type.route()+" in domain: " + domainUri +" ..");

        // Get topics in domain
        String topicUris = udm.find(Resource.Type.TOPIC).from(Resource.Type.DOMAIN, domainUri)
                .stream()
                .map(resource -> "'" + resource.getUri() + "'").collect(Collectors.joining(", "));


        // Create a Data Frame from Cassandra query
        CassandraSQLContext cc = new CassandraSQLContext(sparkHelper.getContext().sc());

        // Define a schema
        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField("starturi", DataTypes.StringType, false),
                        DataTypes.createStructField("enduri", DataTypes.StringType, false),
                        DataTypes.createStructField("weight", DataTypes.DoubleType, false)
                });


        String whereClause = "enduri in (" + topicUris + ")";

        DataFrame df = cc
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(schema)
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode","DROPMALFORMED")
                .options(ImmutableMap.of("table", "dealswith", "keyspace", "research"))
                .load()
                .where(whereClause)
                ;


        LOG.info("getting topic distributions of " + type.route() + " in domain: " + domainUri);

        int estimatedPartitions = helper.getPartitioner().estimatedFor(df);

        // Filter by resource type
        JavaPairRDD<String, List<Relationship>> td = df.toJavaRDD()
                .filter(row -> String.valueOf(row.get(0)).contains("/" + type.route() + "/"))
                .mapToPair(row -> new Tuple2<String, Tuple2<String, Double>>((String) row.get(0), new Tuple2<String,
                        Double>((String) row.get(1), (Double) row.get(2))))
                .groupByKey()
                .repartition(estimatedPartitions)
                .mapValues(el -> StreamSupport.stream(el.spliterator(), false).map(tuple -> new Relationship(tuple
                        ._1, tuple._2)).collect(Collectors.toList()));

        LOG.info("combining pairs of " + type.route() + " ..");

        JavaPairRDD<Tuple2<String, List<Relationship>>, Tuple2<String, List<Relationship>>> tdPairs = td
                .cartesian(td).filter(x -> x._1()._1.compareTo(x._2()._1) > 0);


        LOG.info("calculating similarity score between pairs of " + type.route() + " ..");

        List<Relation> relations = tdPairs
                .map(pair -> RelationBuilder.newSimilarTo(type, pair._1._1, pair._2._1, domainUri,
                        SimilarityCalculator.between(pair._1._2, pair._2._2)))
                .collect();

        // Save in database
        ParallelExecutor executor = new ParallelExecutor();

        if (relations.size() > 0){
            LOG.info("saving " + relations.size() + " SIMILAR_TO relations between "+ type.route() + " in database...");

            relations.forEach(relation -> executor.execute(() -> udm.save(relation)));

            executor.waitFor();
        }

        LOG.info(relations.size() + " similarities between " + type.route() + " discovered!!");
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

//        LOG.info("Deleting previous similar-to relations from column-database...");
//
//        columnRepository.findBy(Relation.Type.SIMILAR_TO_ITEMS, "domain", domainUri).forEach(rel-> {
//            Relation.Type relType = similarFrom(URIGenerator.typeFrom(rel.getStartUri()));
//            columnRepository.delete(relType,rel.getUri());
//        });

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
