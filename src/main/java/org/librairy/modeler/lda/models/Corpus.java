package org.librairy.modeler.lda.models;

import lombok.Data;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

/**
 * Created on 07/07/16:
 *
 * @author cbadenes
 */
@Data
public class Corpus {

    RDD<Tuple2<Object, Vector>> documents;

    CountVectorizerModel model;
}
