/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToTuple implements Serializable, Function<Row, Tuple2<String, double[]>> {

        @Override
        public Tuple2<String, double[]> call(Row row) throws Exception {

            return new Tuple2<String, double[]>(row.getString(0), Doubles.toArray(row.getList(1)));
        }

}
