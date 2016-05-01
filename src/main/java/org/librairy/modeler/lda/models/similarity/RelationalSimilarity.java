package org.librairy.modeler.lda.models.similarity;

import es.upm.oeg.epnoi.matching.metrics.similarity.JensenShannonSimilarity;
import org.apache.spark.mllib.linalg.Vector;
import org.librairy.model.domain.relations.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Created on 01/05/16:
 *
 * @author cbadenes
 */
public class RelationalSimilarity implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(RelationalSimilarity.class);

    public static Double between(List<Relationship> relationships1, List<Relationship> relationships2){

        if (relationships1.isEmpty() || relationships2.isEmpty()) return 0.0;

        Comparator<Relationship> byUri = (e1, e2) ->e1.getUri().compareTo(e2.getUri());

        double[] weights1 = relationships1.stream().sorted(byUri).mapToDouble(x -> x.getWeight()).toArray();
        double[] weights2 = relationships2.stream().sorted(byUri).mapToDouble(x -> x.getWeight()).toArray();

        LOG.debug("weight1: " + Arrays.toString(weights1) + " - weights2:" + Arrays.toString(weights2));

        return JensenShannonSimilarity.apply(weights1, weights2);
    }


    public static Double between (Vector v1, Vector v2){
        return JensenShannonSimilarity.apply(v1.toArray(),v2.toArray());
    }
}
