/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.builder;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.ArrayUtils;
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
import org.librairy.computing.helper.SparkHelper;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.helper.CassandraHelper;
import org.librairy.modeler.lda.helper.SQLHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.boot.storage.UDM;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class DealsBuilder {

    private static Logger LOG = LoggerFactory.getLogger(DealsBuilder.class);

    @Autowired
    UDM udm;

    @Autowired
    URIGenerator uriGenerator;

    @Autowired
    SparkHelper sparkHelper;

    @Autowired
    CassandraHelper cassandraHelper;

    @Autowired
    SQLHelper sqlHelper;

    public void build(Corpus corpus, TopicModel topicModel){

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
                .map(t -> RowFactory.create(t._1, TimeUtils.asISO(), Arrays.asList(ArrayUtils.toObject(t._2.toArray()))));


        LOG.info("saving " + corpus.getSize() + " topic distributions of " + corpus.getTypes()+ " to " +
                "database..");

        // Define a schema
        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField(ShapesDao.RESOURCE_ID, DataTypes.LongType, false),
                        DataTypes.createStructField(ShapesDao.DATE, DataTypes.StringType, false),
                        DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.LongType), false)
                });

        sqlHelper.getContext()
                .createDataFrame(rows, schema)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                .mode(SaveMode.Append)
                .save()
                ;
        LOG.info("saved!");




//
//
//        Tuple2<Object, Vector>[] topicsDistributionArray = (Tuple2<Object, Vector>[]) topicsDistribution.collect();
//
//
//
//        LOG.info("Saving topic distributions of " + corpus.getSize() + " " + corpus.getType().route() + " ..");
//        if (topicsDistributionArray.length>0){
//            ParallelExecutor executor = new ParallelExecutor();
//            for (Tuple2<Object, Vector> distribution: topicsDistributionArray){
//                executor.execute(new Runnable() {
//                    @Override
//                    public void run() {
//                        String uri = documentsUri.get(distribution._1);
//                        double[] weights = distribution._2.toArray();
//                        for (int i = 0; i< weights.length; i++ ){
//
//                            String topicUri = topicRegistry.get(String.valueOf(i));
//                            DealsWith dealsWith;
//                            switch(corpus.getType()){
//                                case ITEM:
//                                    dealsWith = Relation.newDealsWithFromItem(uri,topicUri);
//                                    break;
//                                case PART:
//                                    dealsWith = Relation.newDealsWithFromPart(uri,topicUri);
//                                    break;
//                                case DOCUMENT:
//                                    dealsWith = Relation.newDealsWithFromDocument(uri,topicUri);
//                                    break;
//                                default: continue;
//                            }
//                            dealsWith.setWeight(weights[i]);
//                            LOG.debug("Saving: " + dealsWith);
//                            udm.save(dealsWith);
//                        }
//                    }
//                });
//            }
//            while(!executor.awaitTermination(1, TimeUnit.HOURS)){
//                LOG.warn("waiting for pool ends..");
//            }
//        }
//        LOG.info("Topics distribution of " + corpus.getType().route() + " saved!!");
    }
}
