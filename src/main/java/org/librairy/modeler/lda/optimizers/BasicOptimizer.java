package org.librairy.modeler.lda.optimizers;

import org.librairy.computing.helper.SparkHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    SparkHelper sparkHelper;

    @Override
    public LDAParameters getParametersFor(Corpus corpus) {
        LDAParameters parameters = new LDAParameters();
        parameters.setK(Double.valueOf(2*Math.sqrt(corpus.getSize()/2)).intValue());
        parameters.setBeta(-1.0);
        parameters.setAlpha(-1.0);
        return parameters;
    }
}
