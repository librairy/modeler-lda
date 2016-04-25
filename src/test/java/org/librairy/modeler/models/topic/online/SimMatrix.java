package org.librairy.modeler.models.topic.online;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 14/04/16:
 *
 * @author cbadenes
 */
public class SimMatrix {

    Map<String,Map<String,Double>> matrix;

    public SimMatrix(){
        this.matrix = new HashMap();
    }

    public void add(double similarity, String ref1,String ref2){
        _add(ref1,ref2,similarity);
        _add(ref2,ref1,similarity);
    }

    private void _add(String ref1,String ref2, double value){
        Map<String, Double> similars = matrix.get(ref1);
        if (similars == null){
            similars = new HashMap();
        }
        similars.put(ref2,value);
        matrix.put(ref1,similars);
    }

    public List<String> getSimilarsTo(String ref){
        return matrix.get(ref).entrySet().stream()
                .sorted((x,y) -> x.getValue().compareTo(y.getValue()))
                .map(x -> x.getKey())
                .collect(Collectors.toList());
    }

    public List<String> getSimilarsToGreaterThan(String ref, Double threshold){
        return matrix.get(ref).entrySet().stream()
                .filter(entry -> entry.getValue() >= threshold)
                .sorted((x,y) -> x.getValue().compareTo(y.getValue()))
                .map(x -> x.getKey())
                .collect(Collectors.toList());
    }

    public Map<String,Double> getPatentsFrom(String ref){
        return matrix.get(ref);
    }

}
