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
public class TopicRow implements Serializable{

    private String uri;
    private Long id;
    private String description;
    private List<String> elements;
    private List<Double> scores;
    private String date;
}
