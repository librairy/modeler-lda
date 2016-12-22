/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.optimizers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class LDAOptimizerFactory {

    @Autowired
    List<LDAOptimizer> optimizers;

    public LDAOptimizer by(String id){
        for (LDAOptimizer optimizer: optimizers){
            if (optimizer.getId().equalsIgnoreCase(id)) return optimizer;
        }
        return by("basic");
    }

}
