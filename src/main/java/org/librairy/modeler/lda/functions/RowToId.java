/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import org.apache.spark.sql.Row;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToId extends AbstractFunction1<Row, Tuple2<Long, String>> implements Serializable {

    @Override
    public Tuple2<Long, String> apply(Row v1) {
        Long id     = v1.getLong(0);
        String uri  = v1.getString(1);
        return new Tuple2<Long, String>(id,uri);
    }

}
