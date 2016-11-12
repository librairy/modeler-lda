/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Row;
import org.librairy.modeler.lda.models.InternalResource;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToShape implements Serializable, PairFlatMapFunction<Row, Long, InternalResource> {



        @Override
        public Iterable<Tuple2<Long, InternalResource>> call(Row row) throws Exception {

            return IntStream.range(0, row.getList(2).size())
                    .mapToObj(index ->
                            new Tuple2<Long, InternalResource>(
                                    Long.valueOf(index),
                                    new InternalResource(row.getString(0), row.getLong(1), (Double)
                                            row.getList(2).get(index)))).collect(Collectors.toList());
        }

}
