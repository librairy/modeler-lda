/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.services;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.modeler.lda.graph.DiscoveryPath;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.dao.SimilaritiesDao;
import org.librairy.modeler.lda.graph.PregelSSSP;
import org.librairy.modeler.lda.graph.ShortestPathBuilder;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.collection.JavaConversions;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class SimilarityService {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityService.class);

    @Autowired
    ModelingHelper helper;

    @Autowired
    StorageHelper storageHelper;

    @Autowired
    ShortestPathService shortestPathService;


    public Path[] getShortestPathBetween(ComputingContext context, List<String> startUris, List<String> endUris, List<String> resTypes,
                                         List<String> sectors, Double minScore, Integer maxLength, String domainUri, Integer maxResults) throws IllegalArgumentException{
        try{
            LOG.info("loading nodes..");
            DataFrame shapesDF = loadShapes(context, domainUri, sectors, resTypes).cache();
            long numNodes = shapesDF.count();
            LOG.info(numNodes + " nodes load!");

            LOG.info("loading similarities..");
            DataFrame similaritiesDF = loadSimilarities(context, domainUri, sectors, minScore, resTypes).cache();
            long numSim = similaritiesDF.count();
            LOG.info(numSim + " edges load!");

            LOG.info("discovering shortest path between:  '"+ startUris + "' and '"+endUris+"' in domain: '" +
                    domainUri+"' filtered by " + resTypes + " with min score " + minScore + " and  max " + maxLength + " steps");

            Path[] paths = shortestPathService.calculate(domainUri, startUris, endUris, resTypes, minScore, maxLength, shapesDF, similaritiesDF, maxResults, context.getRecommendedPartitions(), true);

            shapesDF.unpersist();
            similaritiesDF.unpersist();
            return paths;
        } catch (IllegalArgumentException e){
            LOG.warn(e.getMessage());
            return new Path[]{};
        }
    }

    public Path[] getShortestPathBetweenCentroids(ComputingContext context, List<String> startUris, List<String> endUris, Double
            minScore, Integer maxLength, String domainUri, Integer maxResults) throws IllegalArgumentException{

        LOG.info("loading nodes..");
        DataFrame nodesDF = loadCentroids(context, domainUri).cache();
        long numNodes = nodesDF.count();
        LOG.info(numNodes + " nodes load!");

        LOG.info("loading edges..");
        DataFrame edgesDF = loadCentroidSimilarities(context, domainUri, minScore).cache();
        long numSim = edgesDF.count();
        LOG.info(numSim + " edges load!");

        LOG.info("discovering shortest path between centroids:  '"+ startUris + "' and '"+endUris+"' in domain: '" + domainUri+"'" );
        scala.collection.immutable.List<String> start   = JavaConversions.asScalaBuffer(startUris).toList();
        scala.collection.immutable.List<String> end     = JavaConversions.asScalaBuffer(endUris).toList();
        scala.collection.immutable.List<String> types   = JavaConversions.asScalaBuffer(Collections.EMPTY_LIST).toList();
        Path[] paths = DiscoveryPath.apply(start, end, minScore, maxLength, types, nodesDF, edgesDF, maxResults, context.getRecommendedPartitions());


//        Path[] paths = shortestPathService.calculate(domainUri, startUris, endUris, Collections.EMPTY_LIST, minScore, maxLength, nodesDF, edgesDF, maxResults, context.getRecommendedPartitions(), false);

        nodesDF.unpersist();
        edgesDF.unpersist();

        return paths;
    }

    private DataFrame loadCentroids(ComputingContext context, String domainUri) throws IllegalArgumentException {

        if (!storageHelper.exists(storageHelper.path(URIGenerator.retrieveId(domainUri), "lda/similarities/centroids/nodes"))){
            throw new IllegalArgumentException("No centroids found for domain: " + domainUri);
        }

        return loadCentroidsFromFileSystem(context, URIGenerator.retrieveId(domainUri), "nodes");
    }

    private DataFrame loadCentroidSimilarities(ComputingContext context, String domainUri, Double minScore) throws IllegalArgumentException {

        if (!storageHelper.exists(storageHelper.path(URIGenerator.retrieveId(domainUri), "lda/similarities/centroids/edges"))){
            throw new IllegalArgumentException("No centroid-similarities found for domain: " + domainUri);
        }

        DataFrame df = loadCentroidsFromFileSystem(context, URIGenerator.retrieveId(domainUri), "edges").filter(SimilaritiesDao.SCORE + " >= " + minScore);

        Arrays.stream(df.collect()).forEach(row -> LOG.info("ROW: " + row));

        return df;
    }


    public DataFrame loadShapes(ComputingContext context, String domainUri, List<String> sectors, List<String> types) throws IllegalArgumentException {

        DataFrame df = null;
        String previousSector = "";
        for (String sectorId : sectors){
            LOG.info("loading nodes from sector: " + sectorId + " ...");

            DataFrame sectorDF = loadSubgraphFromFileSystem(context, URIGenerator.retrieveId(domainUri), "nodes", sectorId, Optional.empty(), types);
            if (df == null){
                df = sectorDF;
            }else{
                long intersection = df.intersect(sectorDF).count();
                LOG.info(intersection + " nodes in both sectors: [" + previousSector + " and " + sectorId + "]");
                if (intersection == 0l) {
                    throw new IllegalArgumentException("No intersection between [" + previousSector + " and " + sectorId + "]");
                }
                df = df.unionAll(sectorDF).distinct();
            }
            previousSector = sectorId;
        }
        LOG.info("all nodes loaded");
        return df;
    }


    public DataFrame loadSimilarities(ComputingContext context, String domainUri, List<String> sectors, Double minScore, List<String> types) throws IllegalArgumentException {

        DataFrame df = null;

        for (String sectorId : sectors){
            LOG.info("loading edges from sector: " + sectorId + " ...");
            DataFrame sectorDF = loadSubgraphFromFileSystem(context, URIGenerator.retrieveId(domainUri), "edges", sectorId, Optional.of(minScore), types);
            if (df == null){
                df = sectorDF;
            }else{
                df = df.unionAll(sectorDF).distinct();
            }
        }
        LOG.info("all edges loaded");
        return df;
    }

    public void saveCentroids(String domainUri, DataFrame dataFrame){
         try{
             // Clean previous model
             String id = URIGenerator.retrieveId(domainUri);
             storageHelper.create(storageHelper.absolutePath(helper.getStorageHelper().path(id, "")));
             String ldaPath = helper.getStorageHelper().path(id, "lda/similarities/centroids/nodes");
             helper.getStorageHelper().deleteIfExists(ldaPath);

            // Save the model
             String absoluteModelPath = helper.getStorageHelper().absolutePath(helper.getStorageHelper().path(id, "lda/similarities/centroids/nodes"));
             dataFrame
                     .repartition(1)
                     .write()
                     .mode(SaveMode.Overwrite)
                     .save(absoluteModelPath);
             LOG.info("Saved centroids at: " + absoluteModelPath);

        }catch (Exception e){
            if (e instanceof java.nio.file.FileAlreadyExistsException) {
                LOG.warn(e.getMessage());
            }else {
                LOG.error("Error saving model", e);
            }
        }
    }

    public void saveCentroidSimilarities(String domainUri, DataFrame dataFrame){
        try{
            // Clean previous model
            String id = URIGenerator.retrieveId(domainUri);
            storageHelper.create(storageHelper.absolutePath(helper.getStorageHelper().path(id, "")));
            String ldaPath = helper.getStorageHelper().path(id, "lda/similarities/centroids/edges");
            helper.getStorageHelper().deleteIfExists(ldaPath);

            // Save the model
            String absoluteModelPath = helper.getStorageHelper().absolutePath(helper.getStorageHelper().path(id,
                    "lda/similarities/centroids/edges"));
            dataFrame.repartition(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .save(absoluteModelPath);
            LOG.info("Saved centroids at: " + absoluteModelPath);

        }catch (Exception e){
            if (e instanceof java.nio.file.FileAlreadyExistsException) {
                LOG.warn(e.getMessage());
            }else {
                LOG.error("Error saving model", e);
            }
        }
    }


    public void saveSubGraphToFileSystem(DataFrame dataFrame, String id , String label, String centroidId){
        try {
            helper.getStorageHelper().create(id);
            // Clean previous model
            String ldaPath = storageHelper.path(id, "lda/similarities/subgraphs/"+centroidId+"/"+label);
            storageHelper.deleteIfExists(ldaPath);

            // Save the model
            String absoluteModelPath = storageHelper.absolutePath(storageHelper.path(id, "lda/similarities/subgraphs/"+centroidId+"/"+label));
            dataFrame.repartition(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .save(absoluteModelPath);
            LOG.info("Saved subgraph "+centroidId+"/"+label+" from graph-model at: " + absoluteModelPath);

        }catch (Exception e){
            if (e instanceof java.nio.file.FileAlreadyExistsException) {
                LOG.warn(e.getMessage());
            }else {
                LOG.error("Error saving model", e);
            }
        }
    }

    public DataFrame loadSubgraphFromFileSystem(ComputingContext context, String id, String label, String centroidId, Optional<Double> minScore, List<String> types){
        String modelPath = storageHelper.absolutePath(storageHelper.path(id, "lda/similarities/subgraphs/"+centroidId+"/"+label));
        LOG.info("loading subgraph "+centroidId + "/"+ label+" from graph-model:" + modelPath);

        Boolean extendedTypeFilter = (label.equalsIgnoreCase("edges"));

        String typeFilter = extendedTypeFilter?
                "resource_type_1 in (" + types.stream().map(t -> "'"+t+"'").collect(Collectors.joining(",")) + ")" :
                " type in (" + types.stream().map(t -> "'"+t+"'").collect(Collectors.joining(",")) + ")";

        String scoreFilter = (minScore.isPresent())? " score > " + minScore.get() : "";

        StructType structType = (label.equalsIgnoreCase("nodes"))?
                DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
                                DataTypes.createStructField(ShapesDao.RESOURCE_TYPE, DataTypes.StringType, false),
                                DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
                        }) :
                DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType, false),
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType, false),
                                DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType, false),
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_1, DataTypes.StringType, false),
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_2, DataTypes.StringType, false)
                        });

        if (minScore.isPresent() && types.isEmpty()){
            LOG.info("min score and no types filter");
            return context.getSqlContext()
                    .read()
                    .schema(structType)
                    .load(modelPath)
                    .repartition(context.getRecommendedPartitions())
                    .filter(scoreFilter);
        }
        else if (minScore.isPresent() && !types.isEmpty()){
            if (extendedTypeFilter){
                LOG.info("min score and types and extended type filter");
                return context.getSqlContext()
                        .read()
                        .schema(structType)
                        .load(modelPath)
                        .repartition(context.getRecommendedPartitions())
                        .filter(scoreFilter)
                        .filter(typeFilter)
                        .filter("resource_type_2 in (" + types.stream().map(t -> "'"+t+"'").collect(Collectors.joining(",")) + ")");
            }else{
                LOG.info("min score and types and not extended type filter");
                return context.getSqlContext()
                        .read()
                        .schema(structType)
                        .load(modelPath)
                        .repartition(context.getRecommendedPartitions())
                        .filter(scoreFilter)
                        .filter(typeFilter);
            }
        }else if (!minScore.isPresent() && types.isEmpty()){
            LOG.info("no min score and types is empty");
            return context.getSqlContext()
                    .read()
                    .schema(structType)
                    .load(modelPath)
                    .repartition(context.getRecommendedPartitions());
        }else{

            if (extendedTypeFilter){
                LOG.info("else and extended types filter");
                return context.getSqlContext()
                        .read()
                        .schema(structType)
                        .load(modelPath)
                        .repartition(context.getRecommendedPartitions())
                        .filter(typeFilter).filter("resource_type_2 in (" + types.stream().map(t -> "'"+t+"'").collect(Collectors.joining(",")) + ")");
            }else{
                LOG.info("else and else");
                return context.getSqlContext()
                        .read()
                        .schema(structType)
                        .load(modelPath)
                        .repartition(context.getRecommendedPartitions())
                        .filter(typeFilter);
            }
        }
    }

    public void saveToFileSystem(DataFrame dataFrame, String id , String label){
        try {
            helper.getStorageHelper().create(id);
            // Clean previous model
            String ldaPath = storageHelper.path(id, "lda/similarities/graph/"+label);
            storageHelper.deleteIfExists(ldaPath);

            // Save the model
            String absoluteModelPath = storageHelper.absolutePath(storageHelper.path(id, "lda/similarities/graph/"+label));
            dataFrame
                    .repartition(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .save(absoluteModelPath);

            LOG.info("Saved "+label +" from graph-model at: " + absoluteModelPath);

        }catch (Exception e){
            if (e instanceof java.nio.file.FileAlreadyExistsException) {
                LOG.warn(e.getMessage());
            }else {
                LOG.error("Error saving model", e);
            }
        }
    }

    public DataFrame loadFromFileSystem(ComputingContext context, String id, String label){
        String modelPath = storageHelper.absolutePath(storageHelper.path(id,"lda/similarities/graph/"+label));
        LOG.info("loading "+label+" from graph-model:" + modelPath);
        return context.getSqlContext().load(modelPath).repartition(context.getRecommendedPartitions());
    }

    public DataFrame loadCentroidsFromFileSystem(ComputingContext context, String id, String label){
        String modelPath = storageHelper.absolutePath(storageHelper.path(id,"lda/similarities/centroids/"+label));
        LOG.info("loading "+label+" from centroids-graph-model:" + modelPath);

        StructType nodeDataType = (label.equalsIgnoreCase("nodes"))?
                DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
                        DataTypes.createStructField(ShapesDao.RESOURCE_TYPE, DataTypes.StringType, false),
                        DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
                }) :
                DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType, false),
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType, false),
                                DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType, false),
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_1, DataTypes.StringType, false),
                                DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_2, DataTypes.StringType, false)
                        });


        return context.getSqlContext()
                .read()
                .schema(nodeDataType)
                .load(modelPath)
                .repartition(context.getRecommendedPartitions());
    }


}
