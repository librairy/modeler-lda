/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import lombok.Data;

/**
 * Created on 02/09/16:
 *
 * @author cbadenes
 */
@Data
public class TopicDistribution {

    String topicUri;

    Double weight;
}
