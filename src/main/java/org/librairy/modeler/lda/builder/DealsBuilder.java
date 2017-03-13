/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.builder;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class DealsBuilder {

    private static Logger LOG = LoggerFactory.getLogger(DealsBuilder.class);

    @Autowired
    URIGenerator uriGenerator;

    public void build(ComputingContext context, Corpus corpus, TopicModel topicModel){

        String domainUri = uriGenerator.from(Resource.Type.DOMAIN, corpus.getId());

        LOG.info("Generating topic distributions for "+corpus.getTypes()+" in domain: " +
                domainUri);

        // Documents
        RDD<Tuple2<Object, Vector>> documents = corpus.getBagOfWords();

        // LDA Model
        LocalLDAModel localLDAModel = topicModel.getLdaModel();

        JavaRDD<Row> rows = localLDAModel
                .topicDistributions(documents)
                .toJavaRDD()
                .map(t -> RowFactory.create(t._1, TimeUtils.asISO(), Doubles.asList(t._2.toArray())))
                .cache()
                ;

        LOG.info("saving " + corpus.getSize() + " topic distributions of " + corpus.getTypes()+ " to " +
                "database..");

        // Define a schema
        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField(ShapesDao.RESOURCE_ID, DataTypes.LongType, false),
                        DataTypes.createStructField(ShapesDao.DATE, DataTypes.StringType, false),
                        DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
                });

        context.getSqlContext()
                .createDataFrame(rows, schema)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .mode(SaveMode.Append)
                .save()
                ;

        rows.unpersist();

        LOG.info("saved!");
    }
}
