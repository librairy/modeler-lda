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
import org.librairy.modeler.lda.models.Corpus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

/**
 * Created on 01/09/16:
 *
 * @author cbadenes
 */
@Component
//@Conditional(ManualOptimizerCondition.class)
public class ManualOptimizer implements LDAOptimizer{


    @Value("#{environment['LIBRAIRY_LDA_NUM_TOPICS']?:${librairy.lda.numTopics}}")
    Integer numTopics;

    @Value("#{environment['LIBRAIRY_LDA_ALPHA']?:${librairy.lda.alpha}}")
    Double alpha;

    @Value("#{environment['LIBRAIRY_LDA_BETA']?:${librairy.lda.beta}}")
    Double beta;

    @Value("#{environment['LIBRAIRY_LDA_MAX_ITERATIONS']?:${librairy.lda.maxiterations}}")
    Integer maxIterations;

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

        try {
            parameters.setK(Integer.valueOf(parametersDao.get(domainUri,"lda.num.topics")));
        } catch (DataNotFound dataNotFound) {
            parameters.setK(numTopics);
        }

        try{
            parameters.setBeta(Double.valueOf(parametersDao.get(domainUri, "lda.beta")));
        } catch (DataNotFound dataNotFound) {
            parameters.setBeta(beta);
        }

        try{
            parameters.setAlpha(Double.valueOf(parametersDao.get(domainUri, "lda.alpha")));
        } catch (DataNotFound dataNotFound) {
            parameters.setAlpha(alpha);
        }

        try{
            parameters.setIterations(Integer.valueOf(parametersDao.get(domainUri, "lda.max.iterations")));
        } catch (DataNotFound dataNotFound) {
            parameters.setIterations(maxIterations);
        }

        return parameters;
    }
}
