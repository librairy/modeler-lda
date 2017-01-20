/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.librairy.modeler.lda.dao.ShapeRow;

import java.io.Serializable;
import java.util.List;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToArray implements Serializable, Function<Row, double[]> {
    @Override
    public double[] call(Row row) throws Exception {

        List<Double> list   = row.getList(2);
        double[] array      = Doubles.toArray(list);
        return array;
    }


}
