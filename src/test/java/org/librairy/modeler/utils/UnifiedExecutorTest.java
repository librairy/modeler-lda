/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
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
import org.librairy.metrics.utils.Permutations;
import org.librairy.modeler.lda.utils.UnifiedExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class UnifiedExecutorTest {

    private static final Logger LOG = LoggerFactory.getLogger(UnifiedExecutorTest.class);

    @Test
    public void simple() throws InterruptedException {

        UnifiedExecutor executor = new UnifiedExecutor();
        executor.setup();



        for (int i =0; i < 10; i++){
            final Integer id = i;

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    LOG.info("->Thread " + id + " ready to execute task");
                    executor.execute(() -> {
                        LOG.info("## Task from " + id + " executing");
                        try {
                            Thread.currentThread().sleep(20000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
                    LOG.info("-> Executed thread " + id);
                }
            });
            t.run();
        }

        LOG.info("sleeping");
        Thread.currentThread().sleep(60000);
        LOG.info("wake up!");

    }

    @Test
    public void permutations(){

        Permutations<Integer> permutations = new Permutations<>();

        List<Integer> list = Arrays.asList(new Integer[]{1,2,3});

        Set<Pair<Integer>> result = permutations.sorted(list);

        System.out.println(result);


        Collection<List<Integer>> result2 = CollectionUtils.permutations
                (list);

        System.out.println(result2);

    }

    @Test
    public void vectorDistance(){

        double[] a = new double[]{1.0, 1.0, 0.5, 0.5};
        double[] b = new double[]{0.0, 0.0, 0.5, 0.5};


        Vector va = Vectors.dense(a);
        Vector vb = Vectors.dense(b);

        double d1 = Vectors.sqdist(va, vb);
        LOG.info("distance d1: " + d1);

        double d2 = Vectors.sqdist(vb, va);
        LOG.info("distance d2: " + d2);
    }

}
