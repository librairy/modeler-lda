/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.api.model;

import lombok.Data;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class ScoredResource {

    private String uri;

    private String description;

    private Double score;

    public ScoredResource(){}

    public ScoredResource(String uri, String description, Double score){
        this.uri = uri;
        this.description = description;
        this.score = score;
    }
}
