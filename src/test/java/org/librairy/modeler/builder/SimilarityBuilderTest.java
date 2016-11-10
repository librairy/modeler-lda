/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.builder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.google.common.collect.ImmutableMap;
import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.computing.helper.SparkHelper;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.relations.Relationship;
import org.librairy.model.domain.relations.SimilarTo;
import org.librairy.model.domain.resources.Resource;
import org.librairy.model.utils.TimeUtils;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.SimilarityBuilder;
import org.librairy.modeler.lda.functions.SimilarityCalculator;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.DealsWithRow;
import org.librairy.modeler.lda.models.SimilarToRow;
import org.librairy.storage.system.column.templates.ColumnTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created on 27/06/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
public class SimilarityBuilderTest {


    private static final Logger LOG = LoggerFactory.getLogger(SimilarityBuilderTest.class);

    @Autowired
    SimilarityBuilder builder;

    @Autowired
    ModelingHelper modelingHelper;

    @Autowired
    ColumnTemplate columnTemplate;

    @Autowired
    SparkHelper sparkHelper;

    @Autowired
    ModelingHelper helper;

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


    @Test
    public void delete(){
        String domainUri = "http://librairy.org/domains/default";
        builder.delete(domainUri);
    }


    private void calculateSimilaritiesBetweenItems(String domainUri){

        LOG.info("Reading items from domain: " + domainUri);
        List<Resource> items = modelingHelper.getUdm().find(Resource.Type.ITEM).from(Resource.Type.DOMAIN, domainUri);

        JavaRDD<Resource> itemsRDD = modelingHelper.getSparkHelper().getContext().parallelize(items);

        List<Tuple2<Resource, Resource>> itemsPair = itemsRDD.cartesian(itemsRDD)
                .filter(x -> x._1().getUri().compareTo(x._2().getUri()) > 0)
                .collect();

        LOG.info("Calculating similarities...");
        itemsPair.parallelStream().forEach( pair -> {

            List<Relationship> p1 = modelingHelper.getUdm().find(Relation.Type.DEALS_WITH_FROM_ITEM).from(Resource.Type
                    .ITEM, pair._1.getUri()).stream().map(rel -> new Relationship(rel.getEndUri(), rel.getWeight())).collect
                    (Collectors.toList());
            List<Relationship> p2 = modelingHelper.getUdm().find(Relation.Type.DEALS_WITH_FROM_ITEM).from(Resource.Type
                    .ITEM, pair._2.getUri()).stream().map(rel -> new Relationship(rel.getEndUri(), rel.getWeight())).collect
                    (Collectors.toList());

            Double similarity = builder.similarityBetween(p1, p2);

            LOG.info("Attaching SIMILAR_TO based on " + pair);
            SimilarTo simRel1 = Relation.newSimilarToItems(pair._1.getUri(), pair._2.getUri(), domainUri);

            simRel1.setWeight(similarity);
            simRel1.setDomain(domainUri);
            modelingHelper.getUdm().save(simRel1);

        });

    }


    private void calculateSimilaritiesBetweenParts(String domainUri){

        LOG.info("Reading parts from domain: " + domainUri);
        List<Resource> parts = modelingHelper.getUdm().find(Resource.Type.PART).from(Resource.Type.DOMAIN, domainUri);

        JavaRDD<Resource> urisRDD = modelingHelper.getSparkHelper().getContext().parallelize(parts);

        List<Tuple2<Resource, Resource>> pairs = urisRDD.cartesian(urisRDD)
                .filter(x -> x._1().getUri().compareTo(x._2().getUri()) > 0)
                .collect();

        LOG.info("Calculating similarities...");
        pairs.parallelStream().forEach( pair -> {

            List<Relationship> p1 = modelingHelper.getUdm().find(Relation.Type.DEALS_WITH_FROM_PART).from(Resource.Type
                    .PART, pair._1.getUri()).stream().map(rel -> new Relationship(rel.getEndUri(), rel.getWeight())).collect
                    (Collectors.toList());
            List<Relationship> p2 = modelingHelper.getUdm().find(Relation.Type.DEALS_WITH_FROM_PART).from(Resource.Type
                    .PART, pair._2.getUri()).stream().map(rel -> new Relationship(rel.getEndUri(), rel.getWeight())).collect
                    (Collectors.toList());

            Double similarity = builder.similarityBetween(p1, p2);

            LOG.info("Attaching SIMILAR_TO (PART) based on " + pair);
            SimilarTo simRel1 = Relation.newSimilarToParts(pair._1.getUri(), pair._2.getUri(), domainUri);

            simRel1.setWeight(similarity);
            simRel1.setDomain(domainUri);
            modelingHelper.getUdm().save(simRel1);

        });

    }



    @Test
    public void similaritiesFromDataFrame(){



        //Creating Cluster.Builder object
        Cluster.Builder builder1 = Cluster.builder();

        //Adding contact point to the Cluster.Builder object
        Cluster.Builder builder2 = builder1.addContactPoint( "127.0.0.1" );

        //Building a cluster
        Cluster cluster = builder2.build();

        //Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();


        Session session = cluster.connect( );

        //Query
        LOG.info("trying to delete table...");
        String query = "TRUNCATE research.similarto;";
        session.execute(query);
        LOG.info("table deleted!");


        // Create a Data Frame from Cassandra query
        CassandraSQLContext cc = new CassandraSQLContext(sparkHelper.getContext().sc());


//        JavaPairRDD<String, CassandraRow> dealsWithTable = CassandraJavaUtil.javaFunctions(sparkHelper
//                .getContext())
//                .cassandraTable("research", "dealswith")
//                .keyBy(row -> row.getString(4))
//                .repartition(500)
//                ;
//
//
//        JavaPairRDD<String, List<Relationship>> td = dealsWithTable
//                .groupByKey()
//                .mapValues(rows -> StreamSupport.stream(rows.spliterator(), false).map(el -> new Relationship(el
//                        .getString(2),
//                        el.getDouble(5))).collect(Collectors.toList()))
//                .cache();


//        LOG.info("Read: " + td.count() + " elements");

        // Define a schema
        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField("starturi", DataTypes.StringType, false),
                        DataTypes.createStructField("enduri", DataTypes.StringType, false),
                        DataTypes.createStructField("weight", DataTypes.DoubleType, false)
                });


        DataFrame df = cc
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(schema)
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode","DROPMALFORMED")
                .options(ImmutableMap.of("table", "dealswith", "keyspace", "research"))
                .load()
                ;

        JavaRDD<Row> elements = df.toJavaRDD();

        LOG.info("getting topic distributions of elements...");

        //int estimatedPartitions = helper.getPartitioner().estimatedFor(elements);

        int estimatedPartitions = Long.valueOf(elements.count()).intValue()*2;


        // Filter by resource type
        JavaPairRDD<String, List<Relationship>> td = elements
                .mapToPair(row -> new Tuple2<String, Tuple2<String, Double>>((String) row.get(0), new Tuple2<String,
                        Double>((String) row.get(1), (Double) row.get(2))))
                .groupByKey()
                .repartition(estimatedPartitions)
                .mapValues(el -> StreamSupport.stream(el.spliterator(), false).map(tuple -> new Relationship(tuple
                        ._1, tuple._2)).collect(Collectors.toList()))
                .cache()
                ;


//        JavaPairRDD<String, List<Relationship>> td = elements.mapToPair(row -> new Tuple2<String, Tuple2<String,
//                Double>>((String) row.getStarturi(), new Tuple2<String, Double>( (String) row.getEnduri(), (Double) row.getWeight())))
//                .groupByKey()
//                .mapValues(el -> StreamSupport.stream(el.spliterator(), false).map(tuple -> new Relationship(tuple._1, tuple._2)).collect(Collectors.toList()))
//                .cache()
//        ;

        LOG.info("combining pairs of elements ..");

//        int estimatedPartitions = helper.getPartitioner().estimatedFor(td);

        JavaPairRDD<Tuple2<String, List<Relationship>>, Tuple2<String, List<Relationship>>> tdPairs = td
                .cartesian(td)
                .filter(x -> x._1()._1.compareTo(x._2()._1) > 0)
                ;


        LOG.info("calculating similarity score between them ..");
        String domainUri = "http://librairy.org/domains/default";

//        JavaRDD<CassandraRow> similarities = tdPairs.map(pair -> {
//            Map<String, Object> row = new HashMap<String, Object>();
//            row.put("starturi", pair._1._1);
//            row.put("enduri", pair._2._1);
//            row.put("domain", domainUri);
//            row.put("weight", SimilarityCalculator.between(pair._1._2, pair._2._2));
//            return CassandraRow.fromMap(JavaConverters.asScalaMapConverter(row).asScala().toMap(Predef
//                    .<Tuple2<String, Object>>conforms()));
//        });


        JavaRDD<SimilarToRow> similarities = tdPairs.map(pair -> {
            SimilarToRow similarity = new SimilarToRow();
            similarity.setDomain(domainUri);
            similarity.setCreationtime(TimeUtils.asISO());
            similarity.setStarturi(pair._1._1);
            similarity.setEnduri(pair._2._1);
            similarity.setWeight(SimilarityCalculator.between(pair._1._2, pair._2._2));
            int key = new StringBuilder().append(similarity.getStarturi()).append(similarity.getEnduri()).append
                    (similarity.getDomain()).toString().hashCode();
            similarity.setId(key);
            similarity.setUri("http://librairy.org/similarto/" + key);
            return similarity;
        });


        // Save in database
        LOG.info("saving to database..");
        CassandraJavaUtil.javaFunctions(similarities)
                .writerBuilder("research", "similarto", mapToRow(SimilarToRow.class))
                .saveToCassandra();
        LOG.info("saved!");

    }

}
