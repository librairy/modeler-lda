/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
@ToString
public class Node implements Serializable{

    private String uri;

    private Double score;

    public Node(String uri, Double score){
        this.uri = uri;
        this.score = score;
    }
}
