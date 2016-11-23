/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.metrics.data.Ranking;
import org.librairy.metrics.distance.ExtendedKendallsTauDistance;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.TopicRow;
import org.librairy.modeler.lda.dao.TopicsDao;
import org.librairy.modeler.lda.functions.RowToTopic;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Comparison;
import org.librairy.modeler.lda.models.Field;
import org.librairy.modeler.lda.utils.LevenshteinSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDAComparisonTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDAComparisonTask.class);

    public static final String ROUTING_KEY_ID = "lda.correlation.created";

    private final ModelingHelper helper;

    public LDAComparisonTask(ModelingHelper modelingHelper) {
        this.helper = modelingHelper;
    }


    public List<Comparison<Field>> compareTopics(List<String> domains,Integer maxWords, Double minScore){
        if (domains.isEmpty()) return Collections.emptyList();

        JavaRDD<TopicRow> baseTopics = helper.getCassandraHelper().getContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField(TopicsDao.URI, DataTypes.StringType,
                                        false),
                                DataTypes.createStructField(TopicsDao.ELEMENTS, DataTypes.createArrayType
                                                (DataTypes.StringType),
                                        false),
                                DataTypes.createStructField(TopicsDao.SCORES, DataTypes.createArrayType
                                        (DataTypes.DoubleType), false)
                        }))
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode", "DROPMALFORMED")
                .options(ImmutableMap.of("table", TopicsDao.TABLE, "keyspace", SessionManager
                        .getKeyspaceFromUri
                                (domains.get(0))))
                .load()
                .toJavaRDD()
                .map(new RowToTopic(domains.get(0), maxWords));


        for (int i=1; i< domains.size(); i++){
            String partialDomainUri = domains.get(i);
            JavaRDD<TopicRow> partialTopics = helper.getCassandraHelper().getContext()
                    .read()
                    .format("org.apache.spark.sql.cassandra")
                    .schema(DataTypes
                            .createStructType(new StructField[]{
                                    DataTypes.createStructField(TopicsDao.URI, DataTypes.StringType,
                                            false),
                                    DataTypes.createStructField(TopicsDao.ELEMENTS, DataTypes.createArrayType
                                                    (DataTypes.StringType),
                                            false),
                                    DataTypes.createStructField(TopicsDao.SCORES, DataTypes.createArrayType
                                            (DataTypes.DoubleType), false)
                            }))
                    .option("inferSchema", "false") // Automatically infer data types
                    .option("charset", "UTF-8")
                    .option("mode", "DROPMALFORMED")
                    .options(ImmutableMap.of("table", TopicsDao.TABLE, "keyspace", SessionManager
                            .getKeyspaceFromUri
                                    (partialDomainUri)))
                    .load()
                    .toJavaRDD()
                    .map(new RowToTopic(partialDomainUri, maxWords));
            baseTopics = baseTopics.union(partialTopics);
        }


        JavaRDD<TopicRow> topics = baseTopics.cache();


        LOG.info("comparing topics from domains:  " +domains);
        List<Comparison<Field>> comparisons = topics
                .cartesian(topics)
                .filter(pair ->
                        (pair._1.hashCode() < pair._2.hashCode())
                                &&
                                (!pair._1.getDescription().equalsIgnoreCase(pair._2.getDescription())))
                .map(pair -> {
                    Comparison<Field> topicComparison = new Comparison<Field>();

                    Field fieldOne = new Field();
                    fieldOne.setContainerUri(pair._1.getDescription());
                    fieldOne.setFieldUri(pair._1.getUri());
                    topicComparison.setFieldOne(fieldOne);

                    Field fieldTwo = new Field();
                    fieldTwo.setContainerUri(pair._2.getDescription());
                    fieldTwo.setFieldUri(pair._2.getUri());
                    topicComparison.setFieldTwo(fieldTwo);

                    Ranking<String> r1 = new Ranking<String>();
                    Ranking<String> r2 = new Ranking<String>();

                    for (int i = 0; i < maxWords; i++) {
                        r1.add(pair._1.getElements().get(i), pair._1.getScores().get(i));
                        r2.add(pair._2.getElements().get(i), pair._2.getScores().get(i));
                    }

                    Double score = new ExtendedKendallsTauDistance<String>().calculate(r1, r2, new
                            LevenshteinSimilarity());
                    topicComparison.setScore(score);
                    return topicComparison;
                })
                .filter(comparison -> comparison.getScore() > minScore)
                .collect();

        LOG.info(comparisons.size() + " comparisons calculated!");

        return comparisons;
    }

    @Override
    public void run() {

//        helper.getUnifiedExecutor().execute(() -> {
//            try{
//
//
//
//
//
//            }catch (Exception e){
//                if (e instanceof InterruptedException) LOG.warn("Execution canceled");
//                else LOG.error("Error on execution", e);
//            }
//        });

    }


}
