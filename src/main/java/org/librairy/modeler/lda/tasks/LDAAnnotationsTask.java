/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.AnnotationRow;
import org.librairy.modeler.lda.dao.AnnotationsDao;
import org.librairy.modeler.lda.dao.DistributionsDao;
import org.librairy.modeler.lda.dao.TopicsDao;
import org.librairy.modeler.lda.functions.RowToAnnotation;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDAAnnotationsTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDAAnnotationsTask.class);

    public static final String ROUTING_KEY_ID = "lda.annotations.created";

    private final ModelingHelper helper;

    private final String domainUri;

    public LDAAnnotationsTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        helper.getUnifiedExecutor().execute(() -> {
            try{
                DataFrame distributionsDF = helper.getCassandraHelper().getContext()
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


                DataFrame topicsDF = helper.getCassandraHelper().getContext()
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


                LOG.info("generating annotations in domain: " + domainUri + " ..");
                CassandraJavaUtil.javaFunctions(rows)
                        .writerBuilder(SessionManager.getKeyspaceFromUri(domainUri), AnnotationsDao.TABLE, mapToRow
                                (AnnotationRow.class))
                        .saveToCassandra();
                LOG.info("annotation saved!");

                helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(ROUTING_KEY_ID));

            } catch (Exception e){
                // TODO Notify to event-bus when source has not been added
                LOG.error("Error scheduling a new topic model for Items from domain: " + domainUri, e);
            }
        });
        
    }


}
