/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SaveMode;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.resources.Domain;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.metrics.distance.ExtendedKendallsTauSimilarity;
import org.librairy.modeler.lda.dao.ComparisonRow;
import org.librairy.modeler.lda.dao.ComparisonsDao;
import org.librairy.modeler.lda.dao.TopicRank;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.utils.LevenshteinSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDAComparisonTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDAComparisonTask.class);

    public static final String ROUTING_KEY_ID = "lda.comparisons.created";

    private final ModelingHelper helper;

    private final String domainUri;

    public LDAComparisonTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }

    @Override
    public void run() {

        try{
            final ComputingContext context = helper.getComputingHelper().newContext("lda.domains.comparison."+ URIGenerator.retrieveId(domainUri));
            final Integer partitions = context.getRecommendedPartitions();

            helper.getComputingHelper().execute(context, () -> {
                try {

                    LOG.info("Comparing " + domainUri + " with others");

                    // get topics
                    List<TopicRank> topics = helper.getTopicsDao().listAsRank(domainUri, 100);
                    JavaRDD<TopicRank> topicsRDD = context.getSparkContext().parallelize(topics, partitions)
                            .persist(helper.getCacheModeHelper().getLevel());
                    topicsRDD.take(1);//force cache

                    Integer windowSize = 100;
                    Optional<String> offset = Optional.empty();
                    Boolean finished = false;

                    while(!finished){
                        List<Domain> domains = helper.getDomainsDao().list(windowSize,offset,false);

                        for (Domain domain: domains){
                            // get topics per domain
                            List<TopicRank> domainTopics = helper.getTopicsDao().listAsRank(domain.getUri(), 100);
                            JavaRDD<TopicRank> domainTopicsRDD = context.getSparkContext().parallelize(domainTopics, partitions);

                            // compare
                            JavaRDD<ComparisonRow> rows = topicsRDD.cartesian(domainTopicsRDD).map(t -> new ComparisonRow(
                                    t._2.getDomainUri(),
                                    t._2.getTopicUri(),
                                    t._1.getTopicUri(),
                                    TimeUtils.asISO(),
                                    new ExtendedKendallsTauSimilarity<String>().calculate(t._1.getWords(), t._2.getWords(), new LevenshteinSimilarity()))
                            );

                            // save
                            LOG.info("saving comparisons between: " + domainUri + " and " + domain);
                            context.getSqlContext()
                                    .createDataFrame(rows, ComparisonRow.class)
                                    .write()
                                    .format("org.apache.spark.sql.cassandra")
                                    .options(ImmutableMap.of("table", ComparisonsDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                                    .mode(SaveMode.Append)
                                    .save();

                            rows.unpersist();

                            domainTopicsRDD.unpersist();
                        }

                        if (domains.size() < windowSize) break;

                        offset = Optional.of(URIGenerator.retrieveId(domains.get(windowSize-1).getUri()));

                    }

                    LOG.info("Comparisons created for " + domainUri);

                    topicsRDD.unpersist();

                    helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(ROUTING_KEY_ID));

                } catch (Exception e) {
                    if (e instanceof InterruptedException) LOG.warn("Execution canceled");
                    else LOG.error("Error on execution", e);
                }
            });
        } catch (InterruptedException e) {
            LOG.info("Execution interrupted.");
        }

    }

}
