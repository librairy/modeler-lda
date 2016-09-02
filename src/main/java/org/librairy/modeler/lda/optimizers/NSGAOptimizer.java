package org.librairy.modeler.lda.optimizers;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.librairy.computing.helper.SparkHelper;
import org.librairy.metrics.topics.LDASettings;
import org.librairy.metrics.topics.LDASolution;
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
@Conditional(NSGAOptimizerCondition.class)
public class NSGAOptimizer implements LDAOptimizer{

    private static final Logger LOG = LoggerFactory.getLogger(NSGAOptimizer.class);

    @Value("#{environment['LIBRAIRY_LDA_MAX_ITERATIONS']?:${librairy.lda.maxiterations}}")
    Integer maxIterations;

    @Value("#{environment['LIBRAIRY_LDA_MAX_EVALUATIONS']?:${librairy.lda.maxevaluations}}")
    Integer maxEvaluations;

    @Override
    public LDAParameters getParametersFor(Corpus corpus) {

        RDD<Tuple2<Object, Vector>> bagOfWords = corpus.getBagOfWords();

        LOG.info("Ready to execute NSGA (maxIt="+maxIterations+",maxEv="+maxEvaluations+") to search the best " +
                "parameters for the LDA algorithm");
        LDASolution solution = LDASettings.learn(bagOfWords, maxEvaluations, maxIterations);
        LOG.info("NSGA algorithm finished with: " + solution);

        LDAParameters parameters = new LDAParameters();
        parameters.setK(solution.getTopics());
        parameters.setBeta(solution.getBeta());
        parameters.setAlpha(solution.getAlpha());
        return parameters;
    }
}
