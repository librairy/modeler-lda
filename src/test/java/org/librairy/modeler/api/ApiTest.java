/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.api;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.api.ApiConfig;
import org.librairy.modeler.lda.api.LDAModelerAPI;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.api.model.Criteria;
import org.librairy.modeler.lda.api.model.ScoredResource;
import org.librairy.modeler.lda.api.model.ScoredTopic;
import org.librairy.modeler.lda.api.model.ScoredWord;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.dao.SimilaritiesDao;
import org.librairy.modeler.lda.dao.SimilarityRow;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Comparison;
import org.librairy.modeler.lda.models.Field;
import org.librairy.modeler.lda.models.Path;
import org.librairy.modeler.lda.models.Text;
import org.librairy.modeler.lda.services.ShortestPathService;
import org.librairy.modeler.lda.services.SimilarityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ApiConfig.class)
//@TestPropertySource(properties = {
////        "librairy.computing.fs = hdfs://minetur.dia.fi.upm.es:9000"
//})
public class ApiTest {

    private static final Logger LOG = LoggerFactory.getLogger(ApiTest.class);

    @Autowired
    LDAModelerAPI api;

    @Autowired
    ModelingHelper helper;

    @Autowired
    SimilarityService similarityService;

    @Autowired
    ShortestPathService shortestPathService;

    @Test
    public void mostRelevantResources(){

        String topic = "http://librairy.org/topics/f5d024c8871719cd9ae0754d3f4ffbad";
        List<ScoredResource> resources = api.getMostRelevantResources
                (topic, new Criteria());

        LOG.info("Resources: " + resources);

    }


    @Test
    public void similarResourcesFromUri(){

        String uri = "http://librairy.org/parts/5227aa9dbfbe38ad1c8b457f";


        Criteria criteria = new Criteria();
        criteria.setMax(20);
        criteria.setThreshold(0.5);
//        criteria.setTypes(Arrays.asList(new Resource.Type[]{Resource.Type.ITEM}));

        List<ScoredResource> resources = api.getSimilarResources(uri,criteria);

        resources.forEach(res -> LOG.info("Resource: " + res));

    }

    @Test
    public void similarResourcesFromText(){
        String domainUri    = "http://librairy.org/domains/eahb";
        Criteria criteria = new Criteria();
        criteria.setDomainUri(domainUri);
        criteria.setThreshold(0.7);
        criteria.setMax(10);
        criteria.setTypes(Arrays.asList(new Resource.Type[]{Resource.Type.ITEM}));


        Text text = new Text("sample","Remember, as you use CQL, that query planning is not meant to be one of its strengths. Cassandra code typically makes the assumption that you have huge amounts of data, so it will try to avoid doing any queries that might end up being expensive. In the RMDBS world, you structure your data according to intrinsic relationships (3rd normal form, etc), whereas in Cassandra, you structure your data according to the queries you expect to need. Denormalization is (forgive the pun) the norm.");
        List<ScoredResource> resources = api.getSimilarResources(text,criteria);

        resources.forEach(res -> LOG.info("Resource: " + res));
    }

    @Test
    public void compareDomains(){

        List<String> domains = Arrays.asList(new String[]{
           "http://librairy.org/domains/d0fb963ff976f9c37fc81fe03c21ea7b",
           "http://librairy.org/domains/4ba29b9f9e5732ed33761840f4ba6c53"
        });

        List<Comparison<Field>> comparisons = api
                .compareTopicsFrom(domains, new Criteria());

        System.out.println("Comparisons: " + comparisons.size());


        comparisons.forEach(comp -> LOG.info("Comparison: " + comp));
    }


    @Test
    public void tags(){

        String documentUri = "http://librairy.org/documents/bpQYqx9C4HW";
        String itemUri = "http://librairy.org/items/fd798de0e08bdce63359a2074a799877";

        /**
         * http://librairy.org/parts/5227c498bfbe38d7288b5171
         http://librairy.org/parts/5227c498bfbe38d7288b5170
         http://librairy.org/parts/5227c498bfbe38d7288b516f
         http://librairy.org/parts/5227c498bfbe38d7288b516e
         http://librairy.org/parts/5227c498bfbe38d7288b516d
         http://librairy.org/parts/5227c498bfbe38d7288b516c
         http://librairy.org/parts/5227c498bfbe38d7288b516b
         http://librairy.org/parts/5227c498bfbe38d7288b516a
         http://librairy.org/parts/5227c498bfbe38d7288b5169
         http://librairy.org/parts/5227c498bfbe38d7288b5168
         http://librairy.org/parts/5227c498bfbe38d7288b5167
         http://librairy.org/parts/5227c498bfbe38d7288b5166
         http://librairy.org/parts/5227c498bfbe38d7288b5165
         http://librairy.org/parts/5227c498bfbe38d7288b5164
         http://librairy.org/parts/5227c498bfbe38d7288b5163
         http://librairy.org/parts/5227c498bfbe38d7288b5162
         http://librairy.org/parts/5227c498bfbe38d7288b5161
         http://librairy.org/parts/5227c498bfbe38d7288b5160
         http://librairy.org/parts/5227c498bfbe38d7288b515f
         http://librairy.org/parts/5227c498bfbe38d7288b515e
         */

        String resourceUri = "http://librairy.org/parts/5227c498bfbe38d7288b516c";
        List<ScoredWord> tags = api.getTags(resourceUri, new Criteria());

        tags.forEach(tag -> LOG.info("Tag: " + tag));

    }

    @Test
    public void topicsDistribution(){

        String resourceUri = "http://librairy.org/parts/5227bd0abfbe38d7288b4786";
        List<ScoredTopic> tags = api.getTopicsDistribution(resourceUri, new Criteria(),100);

        tags.forEach(el -> LOG.info("Topic: " + el));

    }


    @Test
    public void shortestPath() throws IllegalArgumentException, DataNotFound, InterruptedException {
//        String startUri     = "http://librairy.org/items/yn_0LKzSGSE";
//        String endUri       = "http://librairy.org/items/BrdvqdnF7f-";
//        String domainUri    = "http://librairy.org/domains/141fc5bbcf0212ec9bee5ef66c6096ab";

        String startUri     = "http://librairy.org/items/2-s2.0-41849087732";
        String endUri       = "http://librairy.org/items/2-s2.0-34547850955";
        String domainUri    = "http://librairy.org/domains/eahb";



        Criteria criteria = new Criteria();
        criteria.setDomainUri(domainUri);
        criteria.setThreshold(0.9);
        criteria.setMax(10);

        List<String> types  = Collections.EMPTY_LIST;
        Integer maxLength   = 10;
        Integer maxMinutes  = 10;

        Instant start = Instant.now();
        List<Path> paths = api.getShortestPath(startUri, endUri, types,  maxLength, criteria, maxMinutes);
        Instant end = Instant.now();
        LOG.info("Paths: " + paths);
        LOG.info("Elapsed time: " + Duration.between(start, end).toMillis() + " msecs");

    }

    @Test
    public void similarities() throws InterruptedException {

        String domainUri    = "http://librairy.org/domains/test";
        final ComputingContext context = helper.getComputingHelper().newContext("lda.similarity."+ URIGenerator.retrieveId(domainUri));
        DataFrame dataFrame = context.getCassandraSQLContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
                                DataTypes.createStructField(ShapesDao.RESOURCE_TYPE, DataTypes.StringType, false),
                                DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
                        }))
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode", "DROPMALFORMED")
                .options(ImmutableMap.of("table", ShapesDao.CENTROIDS_TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .load()
                .repartition(context.getRecommendedPartitions())
                .cache()
                ;
        dataFrame.take(1);


        LOG.info("Calculatig centroid-similarities...");

        JavaRDD<SimilarityRow> similarities = dataFrame.toJavaRDD().cartesian(dataFrame.toJavaRDD())
                .filter(r -> !r._1.getString(0).equals(r._2.getString(0)))
                .filter(r -> r._1.getString(1).equals(r._2.getString(1)))
                .map(t -> {
                            SimilarityRow row = new SimilarityRow();
                            row.setResource_uri_1(t._1.getString(0));
                            row.setResource_uri_2(t._2.getString(0));
                            row.setScore(JensenShannonSimilarity.apply(Vectors.dense(Doubles.toArray(t._1.getList(2))).toArray(), Vectors.dense(Doubles.toArray(t._2.getList(2))).toArray()));
                            row.setResource_type_1(t._1.getString(1));
                            row.setResource_type_2(t._2.getString(1));
                            return row;
                        }
                )
                .repartition(context.getRecommendedPartitions())
                .cache()
                ;
        similarities.take(1);

        LOG.info(similarities.count() + " similarities calculated!");

        List<SimilarityRow> byEight = similarities.filter(sr -> sr.getResource_uri_1().equals("8") || sr.getResource_uri_2().equals("8")).collect();
        for (SimilarityRow row : byEight){
            LOG.info("From o To 8: " + row);
        }

        List<SimilarityRow> bySeven = similarities.filter(sr -> sr.getResource_uri_1().equals("7") || sr.getResource_uri_2().equals("7")).collect();
        for (SimilarityRow row : bySeven){
            LOG.info("From o To 7: " + row);
        }

    }

    @Test
    public void fromFS() throws InterruptedException {
        String domainUri    = "http://librairy.org/domains/test";
        final ComputingContext context = helper.getComputingHelper().newContext("lda.similarity."+ URIGenerator.retrieveId(domainUri));

        DataFrame similarities = similarityService.loadCentroidsFromFileSystem(context, URIGenerator.retrieveId(domainUri), "edges");

        List<String> types = Collections.emptyList();


        DataFrame sim;
        if (types.isEmpty()) {
            sim = similarities.filter( SimilaritiesDao.RESOURCE_TYPE_1+ "= '"+ Resource.Type.ANY.name().toLowerCase()+"'");
        }else{
            String filterExpression = types.stream().map(type -> SimilaritiesDao.RESOURCE_TYPE_1 + "= '" + type + "' ").collect(Collectors.joining("or "));

            sim = similarities.filter(filterExpression);
        }

        sim = sim.filter(SimilaritiesDao.RESOURCE_URI_1 + "= '8'");


        for (Row row: sim.collect()){
            LOG.info("Similarity: " + row);
        }

    }

    @Test
    public void shortestPathInMemory() throws InterruptedException {

        String domainUri    = "http://librairy.org/domains/141fc5bbcf0212ec9bee5ef66c6096ab";

        final ComputingContext context = helper.getComputingHelper().newContext("lda.similarity."+ URIGenerator.retrieveId(domainUri));

        DataFrame nodes = context.getCassandraSQLContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
                                DataTypes.createStructField(ShapesDao.RESOURCE_TYPE, DataTypes.StringType, false)
                        }))
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode", "DROPMALFORMED")
                .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .load()
                .repartition(context.getRecommendedPartitions())
                .cache()
                ;
        LOG.info("loading nodes...");
        nodes.take(1);
        LOG.info("done!");


        DataFrame edges = context.getCassandraSQLContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType, false),
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType, false),
                                DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType, false),
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_1, DataTypes.StringType, false),
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_2, DataTypes.StringType, false)
                        }))
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode", "DROPMALFORMED")
                .options(ImmutableMap.of("table", SimilaritiesDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .load()
                .repartition(context.getRecommendedPartitions())
                .cache()
                ;
        LOG.info("loading edges...");
        edges.take(1);
        LOG.info("done!");

        Boolean exit = false;

        while(!exit){

            System.out.print("From:");
            String startId = System.console().readLine();
            String startUri = URIGenerator.fromId(Resource.Type.ITEM, startId);

            System.out.print("To:");
            String endId = System.console().readLine();
            String endUri = URIGenerator.fromId(Resource.Type.ITEM, endId);

            System.out.print("Min Score:");
            String score = System.console().readLine();

            System.out.print("Max Nodes:");
            String max = System.console().readLine();

            Criteria criteria = new Criteria();
            criteria.setDomainUri(domainUri);
            criteria.setThreshold(Double.valueOf(score));
            criteria.setMax(10);

            List<String> types  = Collections.EMPTY_LIST;
            Integer maxLength   = Integer.valueOf(max);
            Integer maxMinutes  = 10;

            edges.filter( SimilaritiesDao.SCORE + " >= " + criteria.getThreshold());

            Instant start = Instant.now();
            Path[] paths = null;
            LOG.info("discovering shortest path between:  '"+ startUri + "' and '"+endUri+"' in domain: '" +
                    domainUri+"' filtered by " + types + " with min score " + criteria.getThreshold() + " and  max " + maxLength + " steps");

            paths = shortestPathService.calculate(domainUri, Arrays.asList(new String[]{startUri}), Arrays.asList(new String[]{startUri}), types, criteria.getThreshold(), maxLength, nodes, edges, criteria.getMax(), context.getRecommendedPartitions(), true);

            Instant end = Instant.now();
            LOG.info("Paths: " + Arrays.asList(paths));
            LOG.info("Elapsed time: " + Duration.between(start, end).toMillis() + " msecs");

        }



    }

}
