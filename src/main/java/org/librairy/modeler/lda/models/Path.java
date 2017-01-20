/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import com.google.common.base.Strings;
import lombok.Data;
import lombok.ToString;
import org.librairy.boot.storage.generator.URIGenerator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
@ToString
public class Path implements Serializable, Comparable {

    String id;

    Double avgScore = 0.0;

    Double accScore = 0.0;

    List<Node> nodes = new ArrayList<Node>();


    public Double getAccScore(){
        return this.accScore;
    }

    public void add(Node node){
        this.nodes.add(node);

        // update id + avg
        if (Strings.isNullOrEmpty(id)){
            id = getNodeDescription(node.getUri());
            avgScore = node.getScore();
        }else{
            id += "->" + getNodeDescription(node.getUri());
            avgScore = (avgScore + node.getScore()) / 2;
        }

        // update acc score
        accScore += node.getScore();

    }

    private String getNodeDescription(String uri){

        if (Strings.isNullOrEmpty(uri) || !uri.contains("/")) return uri;

        return new StringBuilder()
                .append(URIGenerator.typeFrom(uri).key())
                .append("(")
                .append(URIGenerator.retrieveId(uri))
                .append(")")
                .toString()
                ;
    }


    @Override
    public int compareTo(Object o) {
        if (!(o instanceof Path)) return 0;
        Path other = (Path) o;

        int bySize = Integer.valueOf(this.getNodes().size()).compareTo(Integer.valueOf(other.getNodes().size()));

        if (bySize != 0) return bySize;

        List<String> otherNodes = other.getNodes().stream().map(n -> n.getUri()).collect(Collectors.toList());

        long commonNodes = this.getNodes().stream().map(n -> n.getUri()).filter(uri -> otherNodes.contains(uri)).count();

        if (Long.valueOf(commonNodes).intValue() == nodes.size()) return 0;

        return 1;
    }
}
