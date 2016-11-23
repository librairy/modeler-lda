/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.librairy.modeler.lda.models.ResourceShape;
import scala.Tuple2;

import java.io.Serializable;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class TupleToResourceShape implements Serializable, Function<Tuple2<Object, Vector>, ResourceShape> {

    private final String uri;

    public TupleToResourceShape(String uri){
        this.uri = uri;
    }

    @Override
    public ResourceShape call(Tuple2<Object, Vector> objectVectorTuple2) throws Exception {
        ResourceShape shape = new ResourceShape();
        shape.setUri(uri);
        shape.setVector(objectVectorTuple2._2.toArray());
        return shape;
    }
}
