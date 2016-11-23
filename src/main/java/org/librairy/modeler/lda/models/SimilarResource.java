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
 * Created on 02/09/16:
 *
 * @author cbadenes
 */
@Data
public class SimilarResource implements Serializable, Comparable{

    private String uri;

    private Double weight;

    @Override
    public int compareTo(Object o) {

        if (o instanceof SimilarResource){
            return ((SimilarResource)o).getWeight().compareTo(weight);
        }

        return 0;
    }
}
