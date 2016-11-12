/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class DistributionRow implements Serializable{

    private String resource_uri;
    private String resource_type;
    private String topic_uri;
    private String date;
    private Double score;
    private String combined_key;

    public DistributionRow(String resUri, String resType, String topicUri, String date, Double score){
        this.combined_key = resUri+"-"+topicUri;
        this.resource_uri = resUri;
        this.resource_type = resType;
        this.topic_uri = topicUri;
        this.date = date;
        this.score = score;
    }
}
