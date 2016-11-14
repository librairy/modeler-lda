/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.optimizers;

import org.librairy.modeler.lda.models.Corpus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

/**
 * Created on 01/09/16:
 *
 * @author cbadenes
 */
@Component
@Conditional(ManualOptimizerCondition.class)
public class ManualOptimizer implements LDAOptimizer{


    @Value("#{environment['LIBRAIRY_LDA_NUM_TOPICS']?:${librairy.lda.numTopics}}")
    Integer numTopics;

    @Value("#{environment['LIBRAIRY_LDA_ALPHA']?:${librairy.lda.alpha}}")
    Double alpha;

    @Value("#{environment['LIBRAIRY_LDA_BETA']?:${librairy.lda.beta}}")
    Double beta;

    @Override
    public LDAParameters getParametersFor(Corpus corpus) {
        LDAParameters parameters = new LDAParameters();
        parameters.setK(numTopics);
        parameters.setBeta(beta);
        parameters.setAlpha(alpha);
        return parameters;
    }
}
