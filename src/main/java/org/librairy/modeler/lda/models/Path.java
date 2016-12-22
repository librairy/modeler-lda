/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class Path implements Serializable {

    List<Node> nodes = new ArrayList<Node>();

    public void add(Node node){
        this.nodes.add(node);
    }

    public Double getAccumulatedScore(){
        return nodes.stream().reduce( (a,b) -> new Node("",a.getScore()+b.getScore())).get().getScore();
    }

    public Double getAverageScore(){
        return getAccumulatedScore()/Integer.valueOf(nodes.size()).doubleValue();
    }

}
