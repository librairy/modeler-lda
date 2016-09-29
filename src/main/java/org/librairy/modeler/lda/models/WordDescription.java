/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import lombok.Data;

/**
 * Created on 31/08/16:
 *
 * @author cbadenes
 */
@Data
public class WordDescription {

    private String word;

    private Double weight;

    public WordDescription(String word, Double weight){
        this.word = word;
        this.weight = weight;
    }
}
