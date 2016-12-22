/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class ComparisonRow implements Serializable{

    private String domain_uri;
    private String domain_topic_uri;
    private String topic_uri;
    private String date;
    private Double score;

    public ComparisonRow(String domainUri, String domainTopicUri, String topicUri, String date, Double score){
        this.domain_uri = domainUri;
        this.domain_topic_uri = domainTopicUri;
        this.topic_uri = topicUri;
        this.date = date;
        this.score = score;
    }
}
