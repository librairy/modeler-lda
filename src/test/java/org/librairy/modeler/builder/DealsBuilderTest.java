/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.builder;

import org.apache.commons.lang.ArrayUtils;
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
}
