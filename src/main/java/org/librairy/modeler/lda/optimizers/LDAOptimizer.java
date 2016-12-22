/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.optimizers;

import org.librairy.modeler.lda.models.Corpus;

/**
 * Created on 01/09/16:
 *
 * @author cbadenes
 */
public interface LDAOptimizer {

    String getId();

    LDAParameters getParametersFor(Corpus corpus);
}
