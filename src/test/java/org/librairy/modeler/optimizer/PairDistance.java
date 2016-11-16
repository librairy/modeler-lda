/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.optimizer;

import lombok.Data;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class PairDistance {

    private String i;
    private String j;
    private Double distance;


    public PairDistance(String i, String j, Double distance){
        this.i = i;
        this.j = j;
        this.distance = distance;
    }

    public Double getDistanceNormalized(Double max, Double min){
        return (distance - min) / (max - min);
    }
}
