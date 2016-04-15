package org.librairy.modeler.models.topic.online;

import lombok.Data;

import java.io.Serializable;

/**
 * Created on 14/04/16:
 *
 * @author cbadenes
 */
@Data
public class Pair implements Serializable {

    private String t1;
    private String t2;

    public Pair(){}

    public Pair(String t1, String t2){
        this.t1 = t1;
        this.t2 = t2;
    }
}
