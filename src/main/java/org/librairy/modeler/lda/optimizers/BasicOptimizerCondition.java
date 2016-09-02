package org.librairy.modeler.lda.optimizers;

import com.google.common.base.Strings;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Created on 01/09/16:
 *
 * @author cbadenes
 */
public class BasicOptimizerCondition implements Condition {

    @Override
    public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
        String envVar  = System.getenv("LIBRAIRY_LDA_OPTIMIZER");
        return (Strings.isNullOrEmpty(envVar)
                && conditionContext.getEnvironment().getProperty("librairy.lda.optimizer").startsWith("basic"))
                ||
                (!Strings.isNullOrEmpty(envVar) && envVar.startsWith("basic"));
    }
}