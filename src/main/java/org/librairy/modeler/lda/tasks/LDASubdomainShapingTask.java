/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.metrics.aggregation.Bernoulli;
import org.librairy.modeler.lda.dao.ShapeRow;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.functions.RowToArray;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDASubdomainShapingTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDASubdomainShapingTask.class);

    public static final String ROUTING_KEY_ID = "lda.subdomains.shapes.created";

    private final ModelingHelper helper;

    private final String domainUri;

    public LDASubdomainShapingTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        // get subdomains
        try{
            LOG.info("generating shapes for sub-domains of: '" + domainUri + "' ..");

            Iterator<Row> subdomains = getsubdomains();
            // for each ->
            while(subdomains.hasNext()){

                String subdomainUri = subdomains.next().getString(0);
                shapeSubdomain(subdomainUri, domainUri);
            }

            LOG.info("subdomain shapes created from: '" + domainUri + "' ..");
            helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(ROUTING_KEY_ID));
        }catch (Exception e){
            LOG.error("Unexpected error", e);
        }

    }


    private Iterator<Row> getsubdomains(){
        String query = "select uri from subdomains;";

        try{
            ResultSet result = helper.getDbSessionManager().getSessionByUri(domainUri).execute(query.toString());
            return result.iterator();
        }catch (InvalidQueryException e){
            LOG.warn("Error on query: " + query, e.getMessage());
            return Collections.emptyIterator();
        }
    }


    private void shapeSubdomain(String subdomainUri, String domainUri){

        try{
            final ComputingContext context = helper.getComputingHelper().newContext("lda.subdomains."+ URIGenerator.retrieveId(domainUri));
            helper.getComputingHelper().execute(context, () -> {
                try{
                    LOG.info("creating shape for sub-domain '"+ subdomainUri+"'");

                    DataFrame itemsDF = context.getCassandraSQLContext()
                            .read()
                            .format("org.apache.spark.sql.cassandra")
                            .schema(DataTypes
                                    .createStructType(new StructField[]{
                                            DataTypes.createStructField("uri", DataTypes.StringType, false)
                                    }))
                            .option("inferSchema", "false") // Automatically infer data types
                            .option("charset", "UTF-8")
                            .option("mode", "DROPMALFORMED")
                            .options(ImmutableMap.of("table", "items", "keyspace", DBSessionManager.getKeyspaceFromUri(subdomainUri)))
                            .load();


                    DataFrame shapesDF = context.getCassandraSQLContext()
                            .read()
                            .format("org.apache.spark.sql.cassandra")
                            .schema(DataTypes
                                    .createStructType(new StructField[]{
                                            DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
                                            DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
                                    }))
                            .option("inferSchema", "false") // Automatically infer data types
                            .option("charset", "UTF-8")
                            .option("mode", "DROPMALFORMED")
                            .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                            .load();


                    DataFrame distTopicsDF = itemsDF
                            .join(shapesDF, itemsDF.col("uri").equalTo(shapesDF.col(ShapesDao.RESOURCE_URI)));

                    JavaRDD<double[]> rows = distTopicsDF
                            .toJavaRDD()
                            .filter(row -> row.get(2) != null)
                            .map(new RowToArray())
                            .persist(helper.getCacheModeHelper().getLevel());
                            ;

                    LOG.info("generating shape for subdomain: " + subdomainUri + " in domain: " + domainUri + " ..");
                    double[] shape = rows.reduce((a, b) -> Bernoulli.apply(a, b));

                    rows.unpersist();

                    if ((shape != null) && (shape.length > 0)){
                        ShapeRow row = new ShapeRow();
                        row.setUri(subdomainUri);
                        row.setVector(Doubles.asList(shape));
                        row.setId(Long.valueOf(Math.abs(subdomainUri.hashCode())));


                        helper.getShapesDao().save(domainUri, row);
                        LOG.info("shape saved!");
                    }
                } catch (Exception e){
                    // TODO Notify to event-bus when source has not been added
                    LOG.error("Error scheduling a new topic model for Items from domain: " + domainUri, e);
                }
            });
        } catch (InterruptedException e) {
            LOG.info("Execution interrupted.");
        }

    }

}
