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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.modeler.lda.graph.DiscoveryPath;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.dao.SimilaritiesDao;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.collection.JavaConversions;

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


    public Path[] getShortestPathBetween(ComputingContext context, List<String> startUris, List<String> endUris, List<String> resTypes,
                                         List<String> sectors, Double minScore, Integer maxLength, String domainUri, Integer maxResults) throws IllegalArgumentException{

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

        scala.collection.immutable.List<String> start   = JavaConversions.asScalaBuffer(startUris).toList();
        scala.collection.immutable.List<String> end     = JavaConversions.asScalaBuffer(endUris).toList();
        scala.collection.immutable.List<String> types   = JavaConversions.asScalaBuffer(resTypes).toList();
        return DiscoveryPath.apply(start, end, minScore, maxLength, types,  shapesDF, similaritiesDF, maxResults, context.getRecommendedPartitions());
    }

    public Path[] getShortestPathBetweenCentroids(ComputingContext context, List<String> startUris, List<String> endUris, Double
            minScore, Integer maxLength, String domainUri, Integer maxResults) throws IllegalArgumentException{

        LOG.info("loading nodes..");
        DataFrame nodesDF = loadCentroids(context, domainUri).cache();
        nodesDF.take(1);

        LOG.info("loading edges..");
        DataFrame edgesDF = loadCentroidSimilarities(context, domainUri).cache();
        edgesDF.take(1);

        LOG.info("discovering shortest path between centroids:  '"+ startUris + "' and '"+endUris+"' in domain: '" + domainUri+"'" );
        scala.collection.immutable.List<String> start   = JavaConversions.asScalaBuffer(startUris).toList();
        scala.collection.immutable.List<String> end     = JavaConversions.asScalaBuffer(endUris).toList();
        scala.collection.immutable.List<String> types   = JavaConversions.asScalaBuffer(Collections.EMPTY_LIST).toList();
        return DiscoveryPath.apply(start, end, minScore, maxLength, types,  nodesDF, edgesDF, maxResults, context.getRecommendedPartitions());
    }

    private DataFrame loadCentroids(ComputingContext context, String domainUri) throws IllegalArgumentException {

        if (!storageHelper.exists(storageHelper.path(URIGenerator.retrieveId(domainUri), "lda/similarities/centroids/nodes"))){
            throw new IllegalArgumentException("No centroids found for domain: " + domainUri);
        }

        return loadCentroidsFromFileSystem(context, URIGenerator.retrieveId(domainUri), "nodes");
    }

    private DataFrame loadCentroidSimilarities(ComputingContext context, String domainUri) throws IllegalArgumentException {

        if (!storageHelper.exists(storageHelper.path(URIGenerator.retrieveId(domainUri), "lda/similarities/centroids/edges"))){
            throw new IllegalArgumentException("No centroid-similarities found for domain: " + domainUri);
        }

        return loadCentroidsFromFileSystem(context, URIGenerator.retrieveId(domainUri), "edges");
    }


    private DataFrame loadShapes(ComputingContext context, String domainUri, List<String> sectors, List<String> types) throws IllegalArgumentException {

        DataFrame df = null;

        for (String sectorId : sectors){
            LOG.info("loading nodes from sector: " + sectorId + " ...");

            DataFrame sectorDF = loadSubgraphFromFileSystem(context, URIGenerator.retrieveId(domainUri), "nodes", sectorId, Optional.empty(), types);
            if (df == null){
                df = sectorDF;
            }else{
                df = df.unionAll(sectorDF).distinct();
            }
        }
        LOG.info("all nodes loaded");
        return df;
    }


    private DataFrame loadSimilarities(ComputingContext context, String domainUri, List<String> sectors, Double minScore, List<String> types) throws IllegalArgumentException {

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
             dataFrame.save(absoluteModelPath);
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
            dataFrame.save(absoluteModelPath);
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
            dataFrame.save(absoluteModelPath);
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
        String modelPath = storageHelper.absolutePath(storageHelper.path(id,
                "lda/similarities/subgraphs/"+centroidId+"/"+label));
        LOG.info("loading subgraph "+centroidId + "/"+ label+" from graph-model:" + modelPath);

        Boolean extendedTypeFilter = (label.equalsIgnoreCase("edges"));

        String typeFilter = extendedTypeFilter?
                "resource_type_1 in (" + types.stream().map(t -> "'"+t+"'").collect(Collectors.joining(",")) + ")" :
                " type in (" + types.stream().map(t -> "'"+t+"'").collect(Collectors.joining(",")) + ")";

        String scoreFilter = (minScore.isPresent())? " score > " + minScore.get() : "";


        if (minScore.isPresent() && types.isEmpty())
            return context.getCassandraSQLContext().load(modelPath).filter(scoreFilter);
        else if (minScore.isPresent() && !types.isEmpty()){
            if (extendedTypeFilter){
                return context.getCassandraSQLContext().load(modelPath).filter(scoreFilter).filter(typeFilter).filter("resource_type_2 in (" + types.stream().map(t -> "'"+t+"'").collect(Collectors.joining(",")) + ")");
            }else{
                return context.getCassandraSQLContext().load(modelPath).filter(scoreFilter).filter(typeFilter);
            }
        }else if (!minScore.isPresent() && types.isEmpty()){
            return context.getCassandraSQLContext().load(modelPath);
        }else{

            if (extendedTypeFilter){
                return context.getCassandraSQLContext().load(modelPath).filter(typeFilter).filter("resource_type_2 in (" + types.stream().map(t -> "'"+t+"'").collect(Collectors.joining(",")) + ")");
            }else{
                return context.getCassandraSQLContext().load(modelPath).filter(typeFilter);
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
            dataFrame.save(absoluteModelPath);
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
        return context.getCassandraSQLContext().load(modelPath);
    }

    public DataFrame loadCentroidsFromFileSystem(ComputingContext context, String id, String label){
        String modelPath = storageHelper.absolutePath(storageHelper.path(id,"lda/similarities/centroids/"+label));
        LOG.info("loading "+label+" from centroids-graph-model:" + modelPath);
        return context.getCassandraSQLContext().load(modelPath);
    }


}
