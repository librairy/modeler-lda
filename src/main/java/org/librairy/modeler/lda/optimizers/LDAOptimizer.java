package org.librairy.modeler.lda.optimizers;

import org.librairy.modeler.lda.models.Corpus;

/**
 * Created on 01/09/16:
 *
 * @author cbadenes
 */
public interface LDAOptimizer {

    LDAParameters getParametersFor(Corpus corpus);
}
