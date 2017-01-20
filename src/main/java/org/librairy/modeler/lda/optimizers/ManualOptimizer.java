/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.optimizers;

import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.dao.ParametersDao;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.modeler.lda.cache.IterationsCache;
import org.librairy.modeler.lda.cache.ParamAlphaCache;
import org.librairy.modeler.lda.cache.ParamBetaCache;
import org.librairy.modeler.lda.cache.ParamTopicCache;
import org.librairy.modeler.lda.models.Corpus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created on 01/09/16:
 *
 * @author cbadenes
 */
@Component
//@Conditional(ManualOptimizerCondition.class)
public class ManualOptimizer implements LDAOptimizer{


    @Autowired
    ParamTopicCache paramTopicCache;

    @Autowired
    ParamAlphaCache paramAlphaCache;

    @Autowired
    ParamBetaCache paramBetaCache;

    @Autowired
    IterationsCache iterationsCache;

    @Override
    public String getId() {
        return "manual";
    }

    @Autowired
    ParametersDao parametersDao;

    @Override
    public LDAParameters getParametersFor(Corpus corpus) {

        String domainUri = URIGenerator.fromId(Resource.Type.DOMAIN, corpus.getId());

        LDAParameters parameters = new LDAParameters();
        parameters.setK(paramTopicCache.getParameter(domainUri));
        parameters.setBeta(paramBetaCache.getParameter(domainUri));
        parameters.setAlpha(paramAlphaCache.getParameter(domainUri));
        parameters.setIterations(iterationsCache.getIterations(domainUri));
        return parameters;
    }
}
