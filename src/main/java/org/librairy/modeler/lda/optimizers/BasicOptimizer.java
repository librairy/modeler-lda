/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.optimizers;

import org.librairy.modeler.lda.models.Corpus;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

/**
 * Created on 01/09/16:
 *
 * @author cbadenes
 */
@Component
@Conditional(BasicOptimizerCondition.class)
public class BasicOptimizer implements LDAOptimizer{

    @Override
    public LDAParameters getParametersFor(Corpus corpus) {
        LDAParameters parameters = new LDAParameters();
        Integer numTopics = Double.valueOf(2*Math.sqrt(corpus.getSize()/2)).intValue();
        parameters.setK(numTopics != 0? numTopics : 2);
        parameters.setBeta(-1.0);
        parameters.setAlpha(-1.0);
        return parameters;
    }
}
