/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.junit.Test;
import org.librairy.metrics.data.Pair;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.metrics.utils.Permutations;
import org.librairy.modeler.lda.models.Node;
import org.librairy.modeler.lda.models.Path;
import org.librairy.modeler.lda.utils.UnifiedExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class JensenShannonTest {

    private static final Logger LOG = LoggerFactory.getLogger(JensenShannonTest.class);

    @Test
    public void simple() throws InterruptedException {

        double[] p = new double[]{0.000503, 0.000311, 0.015385, 0.000416, 0.013184, 0.000442, 0.000311, 0.000311, 0.000416, 0.00057, 0.000361, 0.000318, 0.000424, 0.000311, 0.000358, 0.00052, 0.000392, 0.000618, 0.000311, 0.001185, 0.000428, 0.000523, 0.000762, 0.000451, 0.000317, 0.00061, 0.000311, 0.002784, 0.000311, 0.000468, 0.000311, 0.011382, 0.000631, 0.001137, 0.001497, 0.002815, 0.000311, 0.000923, 0.000311, 0.000557, 0.000311, 0.000338, 0.000311, 0.000356, 0.001162, 0.001327, 0.001624, 0.00046, 0.004503, 0.000311, 0.000659, 0.000947, 0.000494, 0.000439, 0.000311, 0.000348, 0.00327, 0.002096, 0.000661, 0.000311, 0.001173, 0.000311, 0.001858, 0.000311, 0.00049, 0.000327, 0.00075, 0.000311, 0.000329, 0.000311, 0.000578, 0.000311, 0.000648, 0.000526, 0.000489, 0.000739, 0.000311, 0.00032, 0.000385, 0.000567, 0.00036, 0.003363, 0.000529, 0.000744, 0.000311, 0.000354, 0.000311, 0.000327, 0.000311, 0.00044, 0.002004, 0.000355, 0.001235, 0.000998, 0.000311, 0.000311, 0.862673, 0.000311, 0.000446, 0.00045, 0.000339, 0.005342, 0.001205, 0.000473, 0.001394, 0.001245, 0.000311, 0.002078, 0.000466, 0.000968, 0.000311, 0.000329, 0.000616, 0.000325, 0.000311, 0.008697, 0.000677, 0.000478, 0.000394, 0.000345, 0.000311, 0.001122, 0.001347};

        double[] q = new double[]{9.2e-05, 3.6e-05, 0.035379, 0.000167, 0.015963, 5.2e-05, 3.6e-05, 3.6e-05, 0.001084, 0.000153, 0.000213, 0.000741, 0.000655, 5.4e-05, 0.000452, 0.000773, 0.000419, 0.001004, 0.000172, 0.005817, 0.000313, 0.00099, 0.002229, 0.000247, 0.000196, 0.000831, 0.00238, 0.00735, 3.6e-05, 0.002567, 3.6e-05, 0.493946, 0.00026, 0.000467, 0.000649, 0.004123, 4.1e-05, 0.000595, 3.6e-05, 0.002329, 7e-05, 9.3e-05, 3.6e-05, 6.3e-05, 0.002681, 0.003602, 0.000759, 0.000544, 0.018223, 0.000115, 0.000458, 0.0013, 0.000703, 5e-05, 3.6e-05, 4.3e-05, 0.004473, 0.000385, 0.000885, 3.6e-05, 0.015548, 3.6e-05, 0.007068, 0.000247, 0.000453, 3.7e-05, 0.000225, 0.000114, 0.000347, 3.6e-05, 0.000278, 4.3e-05, 0.000143, 0.000193, 3.6e-05, 0.000196, 4e-05, 3.6e-05, 7.3e-05, 0.001202, 7e-05, 0.011636, 3.6e-05, 0.00173, 0.000165, 6.6e-05, 0.000568, 0.000293, 5.2e-05, 0.000377, 0.003579, 0.000175, 0.003923, 0.00728, 3.6e-05, 3.6e-05, 0.26762, 3.6e-05, 0.000153, 0.000317, 0.000425, 0.010304, 0.003704, 0.000613, 0.00089, 0.006779, 0.000834, 0.00111, 0.000307, 0.001262, 0.000141, 0.000127, 0.004457, 0.000407, 3.6e-05, 0.010161, 0.005499, 0.001062, 0.000245, 7.9e-05, 0.000394, 0.001172, 0.008018};

        double s1 = JensenShannonSimilarity.apply(p, q);
        LOG.info("s1: " + s1);

        double s2 = JensenShannonSimilarity.apply(q, p);
        LOG.info("s2: " + s2);
    }


    @Test
    public void uniqueSet(){

        Set<Path> paths = new TreeSet<Path>();


        Path p1 = new Path();
        p1.add(new Node("1",1.0));
        p1.add(new Node("2",1.0));
        paths.add(p1);


        Path p2 = new Path();
        p2.add(new Node("1",0.6));
        p2.add(new Node("2",0.6));
        p2.add(new Node("3",0.6));
        paths.add(p2);

        LOG.info("Paths: " + paths);


    }



}
