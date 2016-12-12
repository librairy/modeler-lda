/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
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

import java.io.Serializable;
import java.util.List;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToVector implements Serializable, Function<Row, Vector> {

    @Override
    public Vector call(Row row) throws Exception {

        List<Double> listValues = row.getList(1);

        double[] vectorValues = Doubles.toArray(listValues);

        return Vectors.dense(vectorValues);
    }
}
