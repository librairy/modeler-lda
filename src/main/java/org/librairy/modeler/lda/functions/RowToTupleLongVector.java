/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToTupleLongVector implements Serializable, Function<Row, Tuple2<Long, Vector>> {

        @Override
        public Tuple2<Long, Vector> call(Row row) throws Exception {

            return new Tuple2<Long, Vector>(row.getLong(0), Vectors.dense(Doubles.toArray(row.getList(1))));
        }

}
