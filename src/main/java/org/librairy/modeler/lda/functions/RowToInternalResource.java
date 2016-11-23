/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.librairy.modeler.lda.models.InternalResource;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToInternalResource implements Serializable, PairFunction<Row, Long, InternalResource> {



        @Override
        public Tuple2<Long, InternalResource> call(Row row) throws Exception {

            return new Tuple2<Long, InternalResource>(row.getLong(1), new InternalResource(row
                    .getString(0), row.getLong(1), 0.0));
        }

}
