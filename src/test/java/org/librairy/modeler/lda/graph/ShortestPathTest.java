/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.graph;

import com.google.common.primitives.Doubles;
import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.Application;
import org.librairy.modeler.lda.api.ShortestPathAPI;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.dao.SimilaritiesDao;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Centroid;
import org.librairy.modeler.lda.models.Node;
import org.librairy.modeler.lda.models.Path;
import org.librairy.modeler.lda.services.ShortestPathService;
import org.librairy.modeler.lda.services.SimilarityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@TestPropertySource(properties = {
        "librairy.computing.fs = hdfs://minetur.dia.fi.upm.es:9000",
        "librairy.eventbus.host = local"
})
public class ShortestPathTest {

    private static final Logger LOG = LoggerFactory.getLogger(ShortestPathTest.class);

    @Autowired
    ModelingHelper helper;

    @Autowired
    ShortestPathService shortestPathService;

    @Autowired
    SimilarityService similarityService;

    @Autowired
    ShortestPathAPI shortestPathAPI;

    @Test
    public void fromAPI() throws DataNotFound, InterruptedException {
        String startUri     = "http://librairy.org/parts/abs-2-s2.0-35348956577";
        String endUri       = "http://librairy.org/parts/abs-2-s2.0-0034215531";
        String domainUri    = "http://librairy.org/domains/eahb";


        Double minScore     = 0.8;
        Integer maxLength   = 10;
        Integer maxResults  = 5;
        List<String> types  = Arrays.asList(new String[]{"item","part"});
        List<Path> paths = shortestPathAPI.calculate(startUri, endUri, domainUri, minScore, maxLength, maxResults, types);

        for(Path path: paths){
            LOG.info("Found path: " + path);
        }

        LOG.info("Completed!");

    }

    @Test
    public void pipeline() throws InterruptedException, DataNotFound {

//        String startUri     = "http://librairy.org/items/bo7-d_garyckm";
//        String endUri       = "http://librairy.org/items/7d7okvruvsq";
//        String domainUri    = "http://librairy.org/domains/141fc5bbcf0212ec9bee5ef66c6096ab";

//        String startUri     = "http://librairy.org/items/2-s2.0-3242755782";
//        String endUri       = "http://librairy.org/items/2-s2.0-84876305556";
        String startUri     = "http://librairy.org/parts/abs-2-s2.0-35348956577";
        String endUri       = "http://librairy.org/parts/abs-2-s2.0-0034215531";
        String domainUri    = "http://librairy.org/domains/eahb";


        Double minScore     = 0.8;
        Integer maxLength   = 10;
        Integer maxResults  = 5;
        List<String> types  = Collections.emptyList();
//        List<String> types  = Arrays.asList(new String[]{"part"});
        Double minCentroidScore  = minScore / 2.0;


        final ComputingContext context = helper.getComputingHelper().newContext("lda.similarity."+ URIGenerator.retrieveId(domainUri));

        // get centroids from domain
        DataFrame centroids = readCentroids(context, domainUri, types).repartition(context.getRecommendedPartitions());//.cache();
        long numCentroids = centroids.count();
        LOG.info(numCentroids + " centroids loaded from domain: " + domainUri);

        //TODO project centroids by dimension

//        // calculate similarities between centroids
//        LOG.info("calculating similarities between centroids from domain: " + domainUri + " with minimum score: " + minCentroidScore);
//        StructType edgeDataType = DataTypes
//                .createStructType(new StructField[]{
//                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType, false),
//                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType, false),
//                        DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType, false),
//                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_1, DataTypes.StringType, false),
//                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_2, DataTypes.StringType, false)
//                });
//        JavaRDD<Row> centroidSimilarities = centroids.toJavaRDD()
//                .cartesian(centroids.toJavaRDD())
////                .filter(t -> t._1.getString(0).hashCode() < t._2.getString(0).hashCode())
//                .filter( t -> !t._1.getString(0).equalsIgnoreCase(t._2.getString(0)))
//                .map(t -> RowFactory.create(
//                        t._1.getString(0),
//                        t._2.getString(0),
//                        JensenShannonSimilarity.apply(Vectors.dense(Doubles.toArray(t._1.getList(2))).toArray(), Vectors.dense(Doubles.toArray(t._2.getList(2))).toArray()),
//                        t._1.getString(1),
//                        t._2.getString(1)))
//                .filter(r -> r.getDouble(2) > minCentroidScore)
//                ;
//
//        LOG.info(centroidSimilarities.count() + " links between centroids");
//
//        // shortest path between centroids
        DataFrame centroidNodes = centroids.select(ShapesDao.RESOURCE_URI, ShapesDao.RESOURCE_TYPE).toDF("id", "type");

//        DataFrame centroidEdges = context.getCassandraSQLContext().createDataFrame(centroidSimilarities, edgeDataType);
        DataFrame centroidEdges = similarityService.loadCentroidsFromFileSystem(context, URIGenerator.retrieveId(domainUri), "edges").filter(SimilaritiesDao.SCORE + " >= " + minCentroidScore);

        List<Centroid> startCentroids = readClustersOf(context, domainUri, startUri);
        LOG.info("Start centroids: " + startCentroids);
        List<Centroid> endCentroids = readClustersOf(context, domainUri, endUri);
        LOG.info("End centroids: " + endCentroids);
        List<String> startC = startCentroids.stream().map(c -> c.getId().toString()).collect(Collectors.toList());
        List<String> endC = endCentroids.stream().map(c -> c.getId().toString()).collect(Collectors.toList());

        Path[] centroidPaths = shortestPathService.calculate(
                domainUri,
                startC,
                endC,
                Collections.EMPTY_LIST,
                minScore,
                maxLength,
                centroidNodes,
                centroidEdges,
                maxResults,
                context.getRecommendedPartitions(),
                false
        );

        centroids.unpersist();


        for (Path centroidPath: centroidPaths){
            LOG.info("Trying by using the centroid-path: " + centroidPath);

            Path[] spaths = shortestPath(context, domainUri, centroidPath.getNodes(), types, Arrays.asList(new String[]{startUri}), Arrays.asList(new String[]{endUri}), minScore, maxLength, maxResults);

            List<Path> result = Arrays.stream(spaths).filter(p -> p.getNodes().size() < maxLength).collect(Collectors.toList());

            if (!result.isEmpty()){
                LOG.info("Found paths!! => "  + result);
                return;
            }
        }
        LOG.info("No path found!");

    }

    private Path[] shortestPath(ComputingContext context, String domainUri, List<Node> clusters, List<String> types, List<String> startUris, List<String> endUris, Double minScore, Integer maxLength, Integer maxResults){
        if ((clusters == null) || (clusters.isEmpty())) return new Path[]{};

        LOG.info("Start Uris: " + startUris);
        LOG.info("End Uris: " + endUris);

        Tuple2<DataFrame, DataFrame> simGraph = composeSimilarityGraph(context, domainUri, clusters, types, null, null, null, minScore);

        DataFrame nodes = simGraph._1.select(ShapesDao.RESOURCE_URI, ShapesDao.RESOURCE_TYPE).repartition(context.getRecommendedPartitions()).cache();
        long numNodes = nodes.count();
        LOG.info(numNodes + " nodes load!");

        DataFrame edges = simGraph._2.repartition(context.getRecommendedPartitions()).cache();
        long numSim = edges.count();
        LOG.info(numSim + " edges load!");

        List<String> resTypes = Collections.emptyList();

        LOG.info("discovering shortest path between:  '"+ startUris + "' and '"+endUris+"' in domain: '" +
                domainUri+"' filtered by " + resTypes + " with min score " + minScore + " and  max " + maxLength + " steps");

        return shortestPathService.calculate(domainUri, startUris, endUris, resTypes, minScore, maxLength, nodes, edges, maxResults, context.getRecommendedPartitions(), true);

    }

    private List<Centroid> readClustersOf(ComputingContext context, String domainUri, String resourceUri) throws DataNotFound {
        return helper.getClusterDao().getClusters(domainUri, resourceUri)
                .stream().map(id -> {
                    Centroid centroid = new Centroid();
                    centroid.setId(id);
                    return centroid;
                }).collect(Collectors.toList());
    }

    private DataFrame readCentroids(ComputingContext context, String domainUri, List<String> types){
        // ============================

        DataFrame centroids = similarityService.loadCentroidsFromFileSystem(context, URIGenerator.retrieveId(domainUri), "nodes");

        if (types.isEmpty()) return centroids.filter( ShapesDao.RESOURCE_TYPE + "= '"+ Resource.Type.ANY.name().toLowerCase()+"'");

        String filterExpression = types.stream().map(type -> ShapesDao.RESOURCE_TYPE + "= '" + type + "' ").collect(Collectors.joining("or"));

        return centroids.filter(filterExpression);

        // ============================

//        return context.getCassandraSQLContext()
//                .read()
//                .format("org.apache.spark.sql.cassandra")
//                .schema(DataTypes
//                        .createStructType(new StructField[]{
//                                DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
//                                DataTypes.createStructField(ShapesDao.RESOURCE_TYPE, DataTypes.StringType, false),
//                                DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
//                        }))
//                .option("inferSchema", "false") // Automatically infer data types
//                .option("charset", "UTF-8")
//                .option("mode", "DROPMALFORMED")
//                .options(ImmutableMap.of("table", ShapesDao.CENTROIDS_TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
//                .load();
    }

    private DataFrame readResources(ComputingContext context, String domainUri, String clusterId, List<String> types){

        // ============================


        return similarityService.loadSubgraphFromFileSystem(context, URIGenerator.retrieveId(domainUri), "nodes", clusterId, Optional.empty(), types);

        // ============================

//        DataFrame clusterDF = context.getCassandraSQLContext()
//                .read()
//                .format("org.apache.spark.sql.cassandra")
//                .schema(DataTypes
//                        .createStructType(new StructField[]{
//                                DataTypes.createStructField(ClusterDao.URI, DataTypes.StringType, false),
//                                DataTypes.createStructField(ClusterDao.CLUSTER, DataTypes.LongType, false)
//                        }))
//                .option("inferSchema", "false") // Automatically infer data types
//                .option("charset", "UTF-8")
//                .option("mode", "DROPMALFORMED")
//                .options(ImmutableMap.of("table", ClusterDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
//                .load()
//                .filter(ClusterDao.CLUSTER + " = " + clusterId);
//
//        DataFrame shapesDF = context.getCassandraSQLContext()
//                .read()
//                .format("org.apache.spark.sql.cassandra")
//                .schema(DataTypes
//                        .createStructType(new StructField[]{
//                                DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
//                                DataTypes.createStructField(ShapesDao.RESOURCE_TYPE, DataTypes.StringType, false),
//                                DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
//                        }))
//                .option("inferSchema", "false") // Automatically infer data types
//                .option("charset", "UTF-8")
//                .option("mode", "DROPMALFORMED")
//                .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
//                .load();
//
//        DataFrame resourcesDF = shapesDF.join(clusterDF, clusterDF.col(ClusterDao.URI).equalTo(shapesDF.col(ShapesDao.RESOURCE_URI)))
//                .select(shapesDF.col(ShapesDao.RESOURCE_URI), shapesDF.col(ShapesDao.RESOURCE_TYPE), shapesDF.col(ShapesDao.VECTOR))
//                .repartition(context.getRecommendedPartitions()).cache();
//        long numResources = resourcesDF.count();
//        LOG.info(numResources + " resources at cluster: " + clusterId);
//        return resourcesDF;
    }

    private Tuple2<DataFrame,DataFrame> composeSimilarityGraph(ComputingContext context, String domainUri, List<Node> clusters, List<String> types, DataFrame nodes, DataFrame edges, DataFrame neighbours, Double minScore){

        LOG.info("Composing similarity graph from clusters: " + clusters);
        if (clusters.isEmpty()) return new Tuple2<>(nodes,edges);

        String currentCluster = clusters.get(0).getUri();

        DataFrame currentNodes = readResources(context, domainUri, currentCluster, types);

        // ==============================

        DataFrame currentEdges = similarityService.loadSubgraphFromFileSystem(context, URIGenerator.retrieveId(domainUri), "edges", currentCluster, Optional.of(minScore), types);

        // ==============================

//        JavaRDD<Row> currentSimilarities = currentNodes.toJavaRDD().cartesian(currentNodes.toJavaRDD())
//                .filter( t -> !t._1.getString(0).equalsIgnoreCase(t._2.getString(0)))
//                .map(t -> RowFactory.create(
//                        t._1.getString(0),
//                        t._2.getString(0),
//                        JensenShannonSimilarity.apply(Vectors.dense(Doubles.toArray(t._1.getList(2))).toArray(), Vectors.dense(Doubles.toArray(t._2.getList(2))).toArray()),
//                        t._1.getString(1),
//                        t._2.getString(1)))
//                .filter(r -> r.getDouble(2) > minScore)
//                ;
//        DataFrame currentEdges = context.getCassandraSQLContext().createDataFrame(currentSimilarities, edgeDataType);

        Row row = currentEdges.first();
        LOG.info("Edge Row: " + row);

        DataFrame newNodes = (nodes == null)? currentNodes : nodes.unionAll(currentNodes);

        DataFrame newEdges = (edges == null)? currentEdges : edges.unionAll(currentEdges);

        if (neighbours != null){
            LOG.info("Calculating similarities between neighbours..");
            JavaRDD<Row> neighbourSimilarities = neighbours.toJavaRDD().cartesian(currentNodes.toJavaRDD())
//                    .filter(t -> t._1.getString(0).hashCode() < t._2.getString(0).hashCode())
                    .filter( t -> !t._1.getString(0).equalsIgnoreCase(t._2.getString(0)))
                    .map(t -> RowFactory.create(
                            t._1.getString(0),
                            t._2.getString(0),
                            JensenShannonSimilarity.apply(Vectors.dense(Doubles.toArray(t._1.getList(2))).toArray(), Vectors.dense(Doubles.toArray(t._2.getList(2))).toArray()),
                            t._1.getString(1),
                            t._2.getString(1)
                    )
                    )
                    .filter(r -> r.getDouble(2) > minScore)
                    ;
            StructType edgeDataType = DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_1, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_2, DataTypes.StringType, false)
                });
            DataFrame neighbourEdges = context.getCassandraSQLContext().createDataFrame(neighbourSimilarities, edgeDataType);
            newEdges = edges.unionAll(currentEdges).unionAll(neighbourEdges);
            neighbours.unpersist();
        }


        return composeSimilarityGraph(context, domainUri, clusters.subList(1,clusters.size()), types, newNodes, newEdges, currentNodes, minScore);



    }

}
