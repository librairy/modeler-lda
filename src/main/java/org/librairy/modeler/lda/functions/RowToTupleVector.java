/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.VectorWithNorm;
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
public class RowToTupleVector implements Serializable, Function<Row, Tuple2<String, Vector>> {

        @Override
        public Tuple2<String, Vector> call(Row row) throws Exception {

            return new Tuple2<String, Vector>(row.getString(0), Vectors.dense(Doubles.toArray(row.getList(1))));
        }

}
