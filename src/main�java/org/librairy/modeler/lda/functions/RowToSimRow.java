/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Row;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.dao.SimilarityRow;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToSimRow implements Serializable, FlatMapFunction<Row, SimilarityRow> {


    private final double[] shape;
    private final String uri;

    public RowToSimRow(String uri, double[] shape){
            this.shape = shape;
            this.uri = uri;
        }


        @Override
        public List<SimilarityRow> call(Row row) throws Exception {

            Vector rowVector = Vectors.dense(Doubles.toArray(row.getList(1)));

            SimilarityRow row1 = new SimilarityRow();
            row1.setResource_uri_1(uri);
            row1.setResource_uri_2(row.getString(0));
            Double score = (!row1.getResource_uri_1().equalsIgnoreCase(row1.getResource_uri_2()))? JensenShannonSimilarity.apply(rowVector.toArray(), shape) : 0.0;
            row1.setScore(score);
            row1.setResource_type_1(URIGenerator.typeFrom(uri).key());
            row1.setResource_type_2(URIGenerator.typeFrom(row.getString(0)).key());
            row1.setDate(TimeUtils.asISO());

            SimilarityRow row2 = new SimilarityRow();
            row2.setResource_uri_2(uri);
            row2.setResource_uri_1(row.getString(0));
            row2.setScore(score);
            row2.setResource_type_2(URIGenerator.typeFrom(uri).key());
            row2.setResource_type_1(URIGenerator.typeFrom(row.getString(0)).key());
            row2.setDate(TimeUtils.asISO());



            return Arrays.asList(new SimilarityRow[]{row1, row2});
        }

}
