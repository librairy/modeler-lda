/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.librairy.modeler.lda.dao.TopicRow;
import org.librairy.modeler.lda.models.InternalResource;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToTopic implements Serializable, Function<Row, TopicRow> {


    private final String domain;
    private final Integer maxSize;

    public RowToTopic(String domainUri, Integer maxSize){
        this.domain = domainUri;
        this.maxSize = maxSize;
    }

    @Override
    public TopicRow call(Row row) throws Exception {
        TopicRow topic = new TopicRow();
        topic.setUri(row.getString(0));
        topic.setDescription(domain);

        List<String> words = row.getList(1);
        topic.setElements(words.subList(0,maxSize));

        List<Double> scores = row.getList(2);
        topic.setScores(scores.subList(0,maxSize));
        return topic;
    }
}
