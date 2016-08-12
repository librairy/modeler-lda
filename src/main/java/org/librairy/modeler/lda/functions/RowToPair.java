package org.librairy.modeler.lda.functions;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToPair extends AbstractFunction1<Row, Tuple2<Object, Vector>> implements Serializable {

    @Override
    public Tuple2<Object, Vector> apply(Row v1) {
        String uri = (String) v1.get(0);
        Vector vector = (Vector) v1.get(1);
        return new Tuple2<Object, Vector>(from(uri), vector);
    }

    public static Long from(String uri){
        return Long.valueOf(uri.hashCode());
    }

}
