/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.model.domain.relations.Relationship;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class SimilarityCalculator implements Serializable {

    public static Double between(List<Relationship> relationships1, List<Relationship> relationships2){

        if ((relationships1.isEmpty() || relationships2.isEmpty())
                || ((relationships1.size() != relationships2.size()
        ))) return 0.0;

        Comparator<Relationship> byUri = (e1, e2) ->e1.getUri().compareTo(e2.getUri());

        double[] weights1 = relationships1.stream().sorted(byUri).mapToDouble(x -> x.getWeight()).toArray();
        double[] weights2 = relationships2.stream().sorted(byUri).mapToDouble(x -> x.getWeight()).toArray();

        return JensenShannonSimilarity.apply(weights1, weights2);
    }

    public static Double between(Iterable<Relationship> relationships1, Iterable<Relationship> relationships2){

        Comparator<Relationship> byUri = (e1, e2) ->e1.getUri().compareTo(e2.getUri());

        double[] weights1 = StreamSupport.stream(relationships1.spliterator(),false)
                .sorted(byUri)
                .mapToDouble(x -> x.getWeight())
                .toArray();
        double[] weights2 = StreamSupport.stream(relationships2.spliterator(),false)
                .sorted(byUri)
                .mapToDouble(x -> x.getWeight())
                .toArray();

        return JensenShannonSimilarity.apply(weights1, weights2);
    }
}
