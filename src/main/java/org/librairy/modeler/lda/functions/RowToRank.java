/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.librairy.metrics.data.Ranking;
import org.librairy.modeler.lda.dao.TopicRank;
import org.librairy.modeler.lda.dao.TopicRow;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToRank implements Serializable, Function<Row, TopicRank> {


    private final String domain;
    private final Integer maxSize;

    public RowToRank(String domainUri, Integer maxSize){
        this.domain = domainUri;
        this.maxSize = maxSize;
    }

    @Override
    public TopicRank call(Row row) throws Exception {

        Instant a = Instant.now();
        TopicRank topicRank = new TopicRank();
        topicRank.setDomainUri(domain);
        topicRank.setTopicUri(row.getString(0));

        List<String> words = row.getList(1);
        List<Double> scores = row.getList(2);
        Ranking<String> ranking = new Ranking<>();
        for (int i=0; i < maxSize; i++){
            ranking.add(words.get(i),scores.get(i));
        }
        topicRank.setWords(ranking);

        Instant b = Instant.now();

//        System.out.println("elapsed mapping time: " + Duration.between(a,b).toMillis() + " msecs");

        return topicRank;
    }
}
