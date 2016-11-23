/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.*;
import org.librairy.modeler.lda.functions.RowToAnnotation;
import org.librairy.modeler.lda.functions.RowToTuple;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDASimilaritiesTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDASimilaritiesTask.class);

    public static final String ROUTING_KEY_ID = "lda.similarities.created";

    private final ModelingHelper helper;

    private final String domainUri;

    public LDASimilaritiesTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        helper.getUnifiedExecutor().execute(() -> {
            try{
                DataFrame shapesDF = helper.getCassandraHelper().getContext()
                        .read()
                        .format("org.apache.spark.sql.cassandra")
                        .schema(DataTypes
                                .createStructType(new StructField[]{
                                        DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType,
                                                false),
                                        DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType
                                                (DataTypes.DoubleType),
                                                false)
                                }))
                        .option("inferSchema", "false") // Automatically infer data types
                        .option("charset", "UTF-8")
                        .option("mode", "DROPMALFORMED")
                        .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", SessionManager
                                .getKeyspaceFromUri
                                        (domainUri)))
                        .load();


                JavaRDD<Tuple2<String, double[]>> shapes = shapesDF
                        .toJavaRDD()
                        .map(new RowToTuple())
                        .cache();

                JavaRDD<SimilarityRow> rows = shapes
                        .cartesian(shapes).filter(pair -> pair._1.hashCode() < pair._2.hashCode())
                        .flatMap(pair -> {
                            List<SimilarityRow> simRows = new ArrayList<SimilarityRow>();

                            SimilarityRow row1 = new SimilarityRow();
                            row1.setDate(TimeUtils.asISO());
                            row1.setResource_uri_1(pair._1._1);
                            row1.setResource_type_1(URIGenerator.typeFrom(pair._1._1).key());
                            row1.setResource_uri_2(pair._2._1);
                            row1.setResource_type_2(URIGenerator.typeFrom(pair._2._1).key());
                            row1.setScore(JensenShannonSimilarity.apply(pair._1._2, pair._2._2));
                            simRows.add(row1);

                            SimilarityRow row2 = new SimilarityRow();
                            row2.setDate(TimeUtils.asISO());
                            row2.setResource_uri_1(pair._2._1);
                            row2.setResource_type_1(URIGenerator.typeFrom(pair._2._1).key());
                            row2.setResource_uri_2(pair._1._1);
                            row2.setResource_type_2(URIGenerator.typeFrom(pair._1._1).key());
                            row2.setScore(JensenShannonSimilarity.apply(pair._1._2, pair._2._2));
                            simRows.add(row2);

                            return simRows;
                        });


                LOG.info("discovering similarities in domain: " + domainUri + "..");
                CassandraJavaUtil.javaFunctions(rows)
                        .writerBuilder(SessionManager.getKeyspaceFromUri(domainUri), SimilaritiesDao.TABLE, mapToRow(SimilarityRow.class))
                        .saveToCassandra();
                LOG.info("similarities saved!");

                helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(ROUTING_KEY_ID));

            } catch (Exception e){
                // TODO Notify to event-bus when source has not been added
                LOG.error("Error scheduling a new topic model for Items from domain: " + domainUri, e);
            }
        });
        
    }


}
