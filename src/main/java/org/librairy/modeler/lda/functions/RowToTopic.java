/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import com.datastax.driver.core.Row;
import org.apache.spark.api.java.function.Function;
import org.librairy.modeler.lda.dao.TopicRow;

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

        List<String> words = row.getList(1,String.class).subList(0,maxSize);
        topic.setElements(words);

        List<Double> scores = row.getList(2,Double.class).subList(0,maxSize);
        topic.setScores(scores);
        return topic;
    }
}
