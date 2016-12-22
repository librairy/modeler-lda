/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.builder;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class DealsBuilderTest {


    @Test
    public void vectorOfDouble(){

        double[] vector = new double[]{0.0000000005,0.00000000006};

        List<Double> list = Arrays.asList(ArrayUtils.toObject(vector));

        System.out.println(list);
    }

    @Test
    public void combinations(){

        Integer n = 10;
        Integer k = 2;

        long result1 = CombinatoricsUtils.stirlingS2(n, n - 1);
        System.out.println("result1: " + result1);

        long result2 = CombinatoricsUtils.factorial(n) / (CombinatoricsUtils.factorial(k) * (CombinatoricsUtils.factorial(n - k)));
        System.out.println("result2: " + result2);


        Assert.assertEquals(result1, result2);



    }
}
