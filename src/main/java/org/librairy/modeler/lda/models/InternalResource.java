/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class InternalResource implements Serializable{

    private String uri;

    private Long id;

    private Double score;

    public InternalResource(String uri, Long id, Double score){
        this.uri = uri;
        this.id = id;
        this.score = score;
    }
}
