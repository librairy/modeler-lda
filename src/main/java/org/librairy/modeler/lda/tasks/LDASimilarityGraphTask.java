/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.dao.SimilaritiesDao;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDASimilarityGraphTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDASimilarityGraphTask.class);

    public static final String ROUTING_KEY_ID = "lda.graph.created";

    private static final int partitions = Runtime.getRuntime().availableProcessors() * 3;

    private final ModelingHelper helper;

    private final String domainUri;

    public LDASimilarityGraphTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        LOG.info("creating similarity-graph from domain: " + domainUri);

        saveNodesToFileSystem();

        saveEdgesToFileSystem();

        LOG.info("similarity-graph created from domain: " + domainUri);

        helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(ROUTING_KEY_ID));
        
    }


    private void saveNodesToFileSystem(){
        helper.getUnifiedExecutor().execute(() -> {
            try{
                LOG.info("creating vertices..");
                DataFrame shapes = helper.getCassandraHelper().getContext()
                        .read()
                        .format("org.apache.spark.sql.cassandra")
                        .schema(DataTypes
                                .createStructType(new StructField[]{
                                        DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType,
                                                false)
                                }))
                        .option("inferSchema", "false") // Automatically infer data types
                        .option("charset", "UTF-8")
    //                        .option("spark.sql.autoBroadcastJoinThreshold","-1")
                        .option("mode", "DROPMALFORMED")
                        .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", SessionManager
                                .getKeyspaceFromUri
                                        (domainUri)))
                        .load()
                        .repartition(partitions);

                helper.getSimilarityService().saveToFileSystem(shapes,URIGenerator.retrieveId(domainUri), "nodes");
            } catch (Exception e){
                // TODO Notify to event-bus when source has not been added
                LOG.error("Error scheduling a new topic model for Items from domain: " + domainUri, e);
            }
        });

    }

    private void saveEdgesToFileSystem(){
        helper.getUnifiedExecutor().execute(() -> {
            try{
                LOG.info("creating edges..");
                DataFrame similarities = helper.getCassandraHelper().getContext()
                        .read()
                        .format("org.apache.spark.sql.cassandra")
                        .schema(DataTypes
                                .createStructType(new StructField[]{
                                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType,
                                                false),
                                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType,
                                                false),
                                        DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType,
                                                false),
                                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_TYPE_2, DataTypes.StringType,
                                                false)
                                }))
                        .option("inferSchema", "false") // Automatically infer data types
                        .option("charset", "UTF-8")
//                        .option("spark.sql.autoBroadcastJoinThreshold","-1")
                        .option("mode", "DROPMALFORMED")
                        .options(ImmutableMap.of("table", SimilaritiesDao.TABLE, "keyspace", SessionManager
                                .getKeyspaceFromUri(domainUri)))
                        .load()
                        .repartition(partitions);

                helper.getSimilarityService().saveToFileSystem(similarities,URIGenerator.retrieveId(domainUri),
                        "edges");

            } catch (Exception e){
                // TODO Notify to event-bus when source has not been added
                LOG.error("Error scheduling a new topic model for Items from domain: " + domainUri, e);
            }
        });

    }

}
