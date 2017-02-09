/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.TagRow;
import org.librairy.modeler.lda.dao.TagsDao;
import org.librairy.modeler.lda.dao.TopicRank;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.utils.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDADomainTagTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDADomainTagTask.class);

    public static final String ROUTING_KEY_ID = "lda.domain.tags.created";

    private final ModelingHelper helper;

    private final String domainUri;

    public LDADomainTagTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        final ComputingContext context = helper.getComputingHelper().newContext("lda.tags."+ URIGenerator.retrieveId(domainUri));
        final Integer partitions = context.getRecommendedPartitions();
            helper.getComputingHelper().execute(context, () -> {

                try{
                    LOG.info("generating tags for domain: '" + domainUri + "' ..");

                    final List<TopicRank> topicRanks = helper.getTopicsDao().listAsRank(domainUri, 100);

                    JavaRDD<TopicRank> trRDD = context.getSparkContext().parallelize(topicRanks,partitions);

                    JavaRDD<Tuple2<String, Double>> wordsRDD = trRDD.flatMap(tr -> tr.getWords().getPairs());

                    JavaPairRDD<String, Double> tagsRDD = wordsRDD.groupBy(el -> el._1).mapValues
                            (el -> ListUtils.reduce(el));

                    // TO ROW
                    JavaRDD<TagRow> rows = tagsRDD.map(el -> {
                        TagRow row = new TagRow();
                        row.setWord(el._1);
                        row.setScore(el._2);
                        return row;
                    });

                    // Save to DB
                    LOG.info("saving tags in domain: " + domainUri + "..");
                    context.getSqlContext()
                            .createDataFrame(rows, TagRow.class)
                            .write()
                            .format("org.apache.spark.sql.cassandra")
                            .options(ImmutableMap.of("table", TagsDao.TABLE, "keyspace", SessionManager.getKeyspaceFromUri(domainUri)))
                            .save();
                    LOG.info("tags saved!");

                    helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(ROUTING_KEY_ID));
                }catch (Exception e){
                    LOG.error("Unexpected error", e);
                }
            });

    }

}
