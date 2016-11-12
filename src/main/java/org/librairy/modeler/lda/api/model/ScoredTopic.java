/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.api.model;

import lombok.Data;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class ScoredTopic {

    private String uri;

    private String relevance;

    private String description;

    private List<String> elements;

    private List<Double> scores;

}
