/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.eventbus;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.modules.BindingKey;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.EventBusSubscriber;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.builder.CorpusBuilder;
import org.librairy.modeler.lda.builder.DealsBuilder;
import org.librairy.modeler.lda.builder.LDABuilder;
import org.librairy.modeler.lda.dao.*;
import org.librairy.modeler.lda.functions.RowToShape;
import org.librairy.modeler.lda.functions.RowToTopic;
import org.librairy.modeler.lda.helper.CassandraHelper;
import org.librairy.modeler.lda.models.InternalResource;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class LdaShapesCreatedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(LdaShapesCreatedEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    CorpusBuilder corpusBuilder;

    @Autowired
    LDABuilder ldaBuilder;

    @Autowired
    DealsBuilder dealsBuilder;

    @Autowired
    CassandraHelper cassandraHelper;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of("lda.shapes.created"), "lda.shapes.created");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.info("lda shapes created event received: " + event);
        try{
            String domainUri = event.to(String.class);

            JavaPairRDD<Long, InternalResource> shapes = cassandraHelper.getContext()
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


            JavaPairRDD<Long, InternalResource> topics = cassandraHelper.getContext()
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
                    .mapToPair(new RowToTopic());

            JavaRDD<DistributionRow> rows = shapes
                    .join(topics)
                    .map(t -> new DistributionRow(
                            t._2._1.getUri(),
                            URIGenerator.typeFrom(t._2._1.getUri()).key(),
                            t._2._2.getUri(),
                            TimeUtils.asISO(),
                            t._2._1.getScore()));

            LOG.info("saving distributions to database..");
            CassandraJavaUtil.javaFunctions(rows)
                    .writerBuilder(SessionManager.getKeyspaceFromUri(domainUri), DistributionsDao.TABLE, mapToRow(DistributionRow.class))
                    .saveToCassandra();
            LOG.info("saved!");

            eventBus.post(Event.from(domainUri), RoutingKey.of("lda.distributions.created"));


        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
