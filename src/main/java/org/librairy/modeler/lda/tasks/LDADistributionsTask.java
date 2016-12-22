/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.DistributionRow;
import org.librairy.modeler.lda.dao.DistributionsDao;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.dao.TopicsDao;
import org.librairy.modeler.lda.functions.RowToShape;
import org.librairy.modeler.lda.functions.RowToInternalResource;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.InternalResource;
import org.librairy.modeler.lda.utils.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDADistributionsTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDADistributionsTask.class);

    public static final String ROUTING_KEY_ID = "lda.distributions.created";

    private final ModelingHelper helper;

    private final String domainUri;

    public LDADistributionsTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        helper.getUnifiedExecutor().execute(() -> {
            try{
                JavaPairRDD<Long, InternalResource> shapes = helper.getCassandraHelper().getContext()
                        .read()
                        .format("org.apache.spark.sql.cassandra")
                        .schema(DataTypes
                                .createStructType(new StructField[]{
                                        DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
                                        DataTypes.createStructField(ShapesDao.RESOURCE_ID, DataTypes.LongType, false),
                                        DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes
                                                .DoubleType), false)
                                }))
                        .option("inferSchema", "false") // Automatically infer data types
                        .option("charset", "UTF-8")
                        .option("mode", "DROPMALFORMED")
                        .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri
                                (domainUri)))
                        .load()
                        .toJavaRDD()
                        .flatMapToPair(new RowToShape());


                JavaPairRDD<Long, InternalResource> topics = helper.getCassandraHelper().getContext()
                        .read()
                        .format("org.apache.spark.sql.cassandra")
                        .schema(DataTypes
                                .createStructType(new StructField[]{
                                        DataTypes.createStructField(TopicsDao.URI, DataTypes.StringType, false),
                                        DataTypes.createStructField(TopicsDao.ID, DataTypes.LongType, false)
                                }))
                        .option("inferSchema", "false") // Automatically infer data types
                        .option("charset", "UTF-8")
                        .option("mode", "DROPMALFORMED")
                        .options(ImmutableMap.of("table", TopicsDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri
                                (domainUri)))
                        .load()
                        .toJavaRDD()
                        .mapToPair(new RowToInternalResource());

                JavaRDD<DistributionRow> rows = shapes
                        .join(topics)
                        .map(t -> new DistributionRow(
                                t._2._1.getUri(),
                                URIGenerator.typeFrom(t._2._1.getUri()).key(),
                                t._2._2.getUri(),
                                TimeUtils.asISO(),
                                t._2._1.getScore()))
                        .cache();


                LOG.info("calculating topic distributions in domain: " + domainUri + "..");
                CassandraJavaUtil.javaFunctions(rows)
                        .writerBuilder(SessionManager.getKeyspaceFromUri(domainUri), DistributionsDao.TABLE, mapToRow(DistributionRow.class))
                        .saveToCassandra();
                LOG.info("topic distributions saved!");

                helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(ROUTING_KEY_ID));

            } catch (Exception e){
                // TODO Notify to event-bus when source has not been added
                LOG.error("Error scheduling a new topic model for Items from domain: " + domainUri, e);
            }
        });
        
    }


}
