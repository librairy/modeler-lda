/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.optimizers;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.dao.ParametersDao;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.helper.SparkHelper;
import org.librairy.metrics.topics.LDASettings;
import org.librairy.metrics.topics.LDASolution;
import org.librairy.modeler.lda.cache.EvaluationsCache;
import org.librairy.modeler.lda.cache.IterationsCache;
import org.librairy.modeler.lda.models.Corpus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;
import scala.Tuple2;

/**
 * Created on 01/09/16:
 *
 * @author cbadenes
 */
@Component
//@Conditional(NSGAOptimizerCondition.class)
public class NSGAOptimizer implements LDAOptimizer{

    private static final Logger LOG = LoggerFactory.getLogger(NSGAOptimizer.class);

    @Autowired
    IterationsCache iterationsCache;

    @Autowired
    EvaluationsCache evaluationsCache;


    @Override
    public String getId() {
        return "nsga";
    }

    @Override
    public LDAParameters getParametersFor(Corpus corpus) {

        String domainUri = URIGenerator.fromId(Resource.Type.DOMAIN, corpus.getId());

        Integer maxIt = iterationsCache.getIterations(domainUri);
        Integer maxEv = evaluationsCache.getEvaluations(domainUri);

        RDD<Tuple2<Object, Vector>> bagOfWords = corpus.getBagOfWords();

        LOG.info("Ready to execute NSGA (maxIt="+maxIt+",maxEv="+maxEv+") to search the best " +
                "parameters for the LDA algorithm");
        LDASolution solution = LDASettings.learn(bagOfWords, maxEv, maxIt);
        LOG.info("NSGA algorithm finished with: " + solution);

        LDAParameters parameters = new LDAParameters();
        parameters.setK(solution.getTopics());
        parameters.setBeta(solution.getBeta());
        parameters.setAlpha(solution.getAlpha());
        parameters.setIterations(maxIt);
        return parameters;
    }
}
