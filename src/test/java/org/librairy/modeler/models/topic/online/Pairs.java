package org.librairy.modeler.models.topic.online;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Created on 14/04/16:
 *
 * @author cbadenes
 */
@Data
public class Pairs {

    private List<Pair> tuples = new ArrayList<>();

    public void add(Pair tuple){
        tuples.add(tuple);
    }

}
