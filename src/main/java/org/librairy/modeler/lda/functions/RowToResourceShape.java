/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.librairy.modeler.lda.models.ResourceShape;

import java.io.Serializable;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class RowToResourceShape implements Serializable, Function<Row, ResourceShape> {
    @Override
    public ResourceShape call(Row row) throws Exception {
        ResourceShape resourceShape = new ResourceShape();
        resourceShape.setUri(row.getString(0));
        List<Double> weights = row.getList(1);
        resourceShape.setVector(Doubles.toArray(weights));
        return resourceShape;
    }
}
