/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.utils;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class ListUtils implements Serializable {


    public static int indexOfMax(List list){
        return list.indexOf(Collections.max(list));
    }

    public static Double reduce(Iterable<Tuple2<String,Double>> iterable){
        Optional<Tuple2<String,Double>> res = StreamSupport.stream(iterable.spliterator(), false).reduce((a, b) -> new Tuple2(a._1, (a._2 + b._2)/2.0));

        if (!res.isPresent()){
            return 0.0;
        }

        return res.get()._2;

    }
}
