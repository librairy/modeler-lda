/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.optimizers;

import lombok.Data;

/**
 * Created on 01/09/16:
 *
 * @author cbadenes
 */
@Data
public class LDAParameters {

    private double alpha;

    private double beta;

    private int k;
}
