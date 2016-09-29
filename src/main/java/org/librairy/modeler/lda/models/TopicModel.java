/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import lombok.Data;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;

/**
 * Created on 30/08/16:
 *
 * @author cbadenes
 */
@Data
public class TopicModel {

    private final String id;

    private final LocalLDAModel ldaModel;

    private final CountVectorizerModel vocabModel;

    public TopicModel(String id, LocalLDAModel ldaModel, CountVectorizerModel vocabModel){
        this.id = id;
        this.ldaModel = ldaModel;
        this.vocabModel = vocabModel;
    }
}
