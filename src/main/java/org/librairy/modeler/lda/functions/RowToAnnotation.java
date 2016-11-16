/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.modeler.lda.dao.AnnotationRow;
import org.librairy.boot.storage.generator.URIGenerator;
import scala.Tuple2;

import java.io.Serializable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
public class RowToAnnotation implements Serializable, FlatMapFunction<Row, AnnotationRow> {
    @Override
    public Iterable<AnnotationRow> call(Row row) throws Exception {
        int maxElements = Double.valueOf(Math.ceil((row.getDouble(2) * 10) + 0.5)).intValue();
        return IntStream.range(0,maxElements)
                    .mapToObj(idx ->
                            new AnnotationRow(
                                    row.getString(0),
                                    URIGenerator.typeFrom(row.getString(0)).name(),
                                    "tag",
                                    (String) row.getList(4).get(idx),
                                    ((Double) row.getList(5).get(idx)*row.getDouble(2)),
                                    TimeUtils.asISO()
                    )).collect(Collectors.toList());
    }


}
