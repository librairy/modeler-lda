/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.resources.Domain;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.metrics.data.Ranking;
import org.librairy.metrics.distance.ExtendedKendallsTauDistance;
import org.librairy.metrics.distance.ExtendedKendallsTauSimilarity;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.*;
import org.librairy.modeler.lda.functions.RowToRank;
import org.librairy.modeler.lda.functions.RowToTopic;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Comparison;
import org.librairy.modeler.lda.models.Field;
import org.librairy.modeler.lda.utils.LevenshteinSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDAComparisonTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDAComparisonTask.class);

    private static final int partitions = Runtime.getRuntime().availableProcessors() * 3;

    public static final String ROUTING_KEY_ID = "lda.comparisons.created";

    private final ModelingHelper helper;

    private final String domainUri;

    public LDAComparisonTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }

    @Override
    public void run() {

        helper.getSparkHelper().execute(() -> {
            try {

                LOG.info("Comparing " + domainUri + " with others");

                // get domains
                List<String> domains = helper.getDomainsDao().listOnly(Domain.URI)
                        .stream()
                        .filter(uri -> !uri.equalsIgnoreCase(domainUri))
                        .collect(Collectors.toList());

                // get topics
                List<TopicRank> topics = helper.getTopicsDao().listAsRank(domainUri, 100);
                JavaRDD<TopicRank> topicsRDD = helper.getSparkHelper().getContext().parallelize(topics, partitions).cache();
                topicsRDD.take(1);//force cache

                for (String domain : domains) {

                    // get topics per domain
                    List<TopicRank> domainTopics = helper.getTopicsDao().listAsRank(domain, 100);
                    JavaRDD<TopicRank> domainTopicsRDD = helper.getSparkHelper().getContext().parallelize(domainTopics, partitions);

                    // compare
                    JavaRDD<ComparisonRow> rows = topicsRDD.cartesian(domainTopicsRDD).map(t -> new ComparisonRow(
                            t._2.getDomainUri(),
                            t._2.getTopicUri(),
                            t._1.getTopicUri(),
                            TimeUtils.asISO(),
                            new ExtendedKendallsTauSimilarity<String>().calculate(t._1.getWords(), t._2.getWords(),
                                    new LevenshteinSimilarity()))
                    );


                    // save
                    LOG.info("saving comparisons between: " + domainUri + " and " + domain);
                    CassandraJavaUtil.javaFunctions(rows)
                            .writerBuilder(
                                    SessionManager.getKeyspaceFromUri(domainUri),
                                    ComparisonsDao.TABLE,
                                    mapToRow(ComparisonRow.class))
                            .saveToCassandra();
                }

                LOG.info("Comparisons created for " + domainUri);

                //TODO publish event
                helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(ROUTING_KEY_ID));

            } catch (Exception e) {
                if (e instanceof InterruptedException) LOG.warn("Execution canceled");
                else LOG.error("Error on execution", e);
            }
        });

    }

}
