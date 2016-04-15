package org.librairy.modeler.models.topic.online;

import lombok.Getter;

import java.io.Serializable;

/**
 * Created on 14/04/16:
 *
 * @author cbadenes
 */
public class WeightedPair implements Serializable {

    @Getter
    String uri1;
    @Getter
    String uri2;
    @Getter
    Double weight;

    public WeightedPair(String uri1, String uri2, Double weight){
        this.uri1 = uri1;
        this.uri2 = uri2;
        this.weight = weight;
    }
}

