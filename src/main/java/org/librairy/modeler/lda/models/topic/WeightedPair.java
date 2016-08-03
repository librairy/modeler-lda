package org.librairy.modeler.lda.models.topic;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Created on 01/05/16:
 *
 * @author cbadenes
 */
@ToString
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