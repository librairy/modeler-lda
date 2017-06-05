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
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.dao.ShapeRow;
import org.librairy.modeler.lda.dao.SimilarityRow;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToSimRow implements Serializable, Function<Row, SimilarityRow> {


    private final Vector shape;
    private final String uri;

    public RowToSimRow(String uri, Vector shape){
            this.shape = shape;
            this.uri = uri;
        }


        @Override
        public SimilarityRow call(Row row) throws Exception {

            Vector rowVector = Vectors.dense(Doubles.toArray(row.getList(1)));

            SimilarityRow row1 = new SimilarityRow();
            row1.setResource_uri_1(uri);
            row1.setResource_uri_2(row.getString(0));
            Double score = (!row1.getResource_uri_1().equalsIgnoreCase(row1.getResource_uri_2()))? JensenShannonSimilarity.apply(rowVector.toArray(), shape.toArray()) : 0.0;
            row1.setScore(score);
            row1.setResource_type_1(URIGenerator.typeFrom(row.getString(0)).key());
            row1.setResource_type_2(URIGenerator.typeFrom(uri).key());
            row1.setDate(TimeUtils.asISO());
            return row1;
        }

}
