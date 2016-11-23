/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.utils;

import org.librairy.metrics.distance.SimilarityMeasure;

import java.io.Serializable;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class LevenshteinSimilarity implements Serializable, SimilarityMeasure<String> {

    @Override
    public Double between(String str1, String str2) {
        int distance        = LevenshteinDistance.computeLevenshteinDistance(str1, str2);
        Double similarity   = 1.0 - (Integer.valueOf(distance).doubleValue() / Math.max(Integer.valueOf(str1.length()
        ).doubleValue(), Integer.valueOf(str2.length()).doubleValue()));
        return similarity;
    }
}
