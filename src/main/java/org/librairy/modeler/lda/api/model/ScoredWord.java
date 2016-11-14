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
public class ScoredWord {

    private String word;

    private Double score;

    public ScoredWord(){}

    public ScoredWord(String word, Double score){
        this.word = word;
        this.score  = score;
    }

}
