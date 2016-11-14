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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.model.Event;
import org.librairy.model.modules.BindingKey;
import org.librairy.model.modules.EventBus;
import org.librairy.model.modules.EventBusSubscriber;
import org.librairy.model.modules.RoutingKey;
import org.librairy.model.utils.TimeUtils;
import org.librairy.modeler.lda.builder.CorpusBuilder;
import org.librairy.modeler.lda.builder.DealsBuilder;
import org.librairy.modeler.lda.builder.LDABuilder;
import org.librairy.modeler.lda.dao.*;
import org.librairy.modeler.lda.functions.RowToAnnotation;
import org.librairy.modeler.lda.functions.RowToShape;
import org.librairy.modeler.lda.functions.RowToTopic;
import org.librairy.modeler.lda.helper.CassandraHelper;
import org.librairy.modeler.lda.models.InternalResource;
import org.librairy.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.annotation.PostConstruct;

import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class LdaDistributionsCreatedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(LdaDistributionsCreatedEventHandler.class);

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
        BindingKey bindingKey = BindingKey.of(RoutingKey.of("lda.distributions.created"), "lda.distributions.created");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.info("lda distributions created event received: " + event);
        try{
            String domainUri = event.to(String.class);

            DataFrame distributionsDF = cassandraHelper.getContext()
                    .read()
                    .format("org.apache.spark.sql.cassandra")
                    .schema(DataTypes
                            .createStructType(new StructField[]{
                                    DataTypes.createStructField(DistributionsDao.RESOURCE_URI, DataTypes.StringType,
                                            false),
                                    DataTypes.createStructField(DistributionsDao.TOPIC_URI, DataTypes.StringType,
                                            false),
                                    DataTypes.createStructField(DistributionsDao.SCORE, DataTypes.DoubleType, false)
                            }))
                    .option("inferSchema", "false") // Automatically infer data types
                    .option("charset", "UTF-8")
                    .option("mode", "DROPMALFORMED")
                    .options(ImmutableMap.of("table", DistributionsDao.TABLE, "keyspace", SessionManager
                            .getKeyspaceFromUri
                                    (domainUri)))
                    .load();


            DataFrame topicsDF = cassandraHelper.getContext()
                    .read()
                    .format("org.apache.spark.sql.cassandra")
                    .schema(DataTypes
                            .createStructType(new StructField[]{
                                    DataTypes.createStructField(TopicsDao.URI, DataTypes.StringType, false),
                                    DataTypes.createStructField(TopicsDao.ELEMENTS, DataTypes.createArrayType
                                            (DataTypes.StringType), false),
                                    DataTypes.createStructField(TopicsDao.SCORES, DataTypes.createArrayType
                                            (DataTypes.DoubleType), false)
                            }))
                    .option("inferSchema", "false") // Automatically infer data types
                    .option("charset", "UTF-8")
                    .option("mode", "DROPMALFORMED")
                    .options(ImmutableMap.of("table", TopicsDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri
                            (domainUri)))
                    .load();


            DataFrame distTopicsDF = distributionsDF
                    .join(topicsDF, distributionsDF.col(DistributionsDao.TOPIC_URI).equalTo(topicsDF.col(TopicsDao.URI)));



            JavaRDD<AnnotationRow> rows = distTopicsDF
                    .toJavaRDD()
                    .flatMap(new RowToAnnotation())
                    ;


            LOG.info("saving annotations to database..");
            CassandraJavaUtil.javaFunctions(rows)
                    .writerBuilder(SessionManager.getKeyspaceFromUri(domainUri), AnnotationsDao.TABLE, mapToRow
                            (AnnotationRow.class))
                    .saveToCassandra();
            LOG.info("saved!");

            eventBus.post(Event.from(domainUri), RoutingKey.of("lda.annotations.created"));


        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
