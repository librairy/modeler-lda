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

import java.util.ArrayList;
import java.util.List;

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


    public Path getShortestPathBetween(String uri1, String uri2, Double minScore, Integer maxLength, String
            domainUri) throws IllegalArgumentException{

        LOG.info("loading shapes..");
        DataFrame shapesDF = loadShapes(domainUri).cache();
//        DataFrame shapesDF = simulateShapes(domainUri).cache();
        shapesDF.take(1);
        Long verticesNum = shapesDF.count();
        LOG.info("number of vertices:  "+ verticesNum );

        LOG.info("loading similarities..");
        DataFrame similaritiesDF = loadSimilarities(domainUri).cache();
//        DataFrame similaritiesDF = simulateSimilarities(domainUri).cache();
        similaritiesDF.take(1);

        Long edgesNum = similaritiesDF.count();
        LOG.info("number of edges:  "+ edgesNum );

        LOG.info("discovering shortest path between:  '"+ uri1 + "' and '"+uri2+"' in domain: '" + domainUri+"'" );
        return DiscoveryPath.apply(uri1, uri2, minScore, maxLength, shapesDF, similaritiesDF);
    }

    private DataFrame loadShapes(String domainUri) throws IllegalArgumentException {

        if (!storageHelper.exists(storageHelper.path(URIGenerator.retrieveId(domainUri),"lda/simgraph/vertices"))){
            throw new IllegalArgumentException("No similarity-graph found for domain: " + domainUri);
        }

        return loadFromFileSystem(URIGenerator.retrieveId(domainUri), "vertices");
    }


    private DataFrame loadSimilarities(String domainUri) throws IllegalArgumentException {

        if (!storageHelper.exists(storageHelper.path(URIGenerator.retrieveId(domainUri),"lda/simgraph/edges"))){
            throw new IllegalArgumentException("No similarity-graph found for domain: " + domainUri);
        }

        return loadFromFileSystem(URIGenerator.retrieveId(domainUri), "edges");
    }

    public void saveToFileSystem(DataFrame dataFrame, String id , String label){
        try {
            // Clean previous model
            String ldaPath = storageHelper.path(id, "lda/simgraph/"+label);
            storageHelper.deleteIfExists(ldaPath);

            // Save the model
            String absoluteModelPath = storageHelper.absolutePath(storageHelper.path(id, "lda/simgraph/"+label));
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

    public DataFrame loadFromFileSystem(String id, String label){
        String modelPath = storageHelper.absolutePath(storageHelper.path(id,"lda/simgraph/"+label));
        LOG.info("loading "+label+" from graph-model:" + modelPath);
        return helper.getCassandraHelper().getContext().load(modelPath);
    }

    private DataFrame simulateShapes(String domainUri){

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType,
                        false)});

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("uri1"));
        rows.add(RowFactory.create("uri2"));
        rows.add(RowFactory.create("uri3"));
        rows.add(RowFactory.create("uri4"));
        rows.add(RowFactory.create("uri5"));
        rows.add(RowFactory.create("uri6"));

        return helper.getCassandraHelper().getContext().createDataFrame(rows,schema);
    }

    private DataFrame simulateSimilarities(String domainUri){

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType,
                        false),
                DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType,
                        false),
                DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType,
                        false)});

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("uri1","uri2",0.9));
        rows.add(RowFactory.create("uri2","uri1",0.9));

        rows.add(RowFactory.create("uri1","uri3",0.7));
        rows.add(RowFactory.create("uri3","uri1",0.7));

        rows.add(RowFactory.create("uri2","uri4",0.9));
        rows.add(RowFactory.create("uri4","uri2",0.9));

        rows.add(RowFactory.create("uri3","uri4",0.8));
        rows.add(RowFactory.create("uri4","uri3",0.8));

        rows.add(RowFactory.create("uri1","uri5",0.4));
        rows.add(RowFactory.create("uri5","uri1",0.4));

        rows.add(RowFactory.create("uri5","uri6",0.5));
        rows.add(RowFactory.create("uri6","uri5",0.5));

        rows.add(RowFactory.create("uri6","uri4",0.8));
        rows.add(RowFactory.create("uri4","uri6",0.8));

        return helper.getCassandraHelper().getContext().createDataFrame(rows,schema);
    }

}
