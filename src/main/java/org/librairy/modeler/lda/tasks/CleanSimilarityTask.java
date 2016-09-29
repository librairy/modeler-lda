/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class CleanSimilarityTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CleanSimilarityTask.class);

    private final ModelingHelper helper;

    private final String domainUri;

    public CleanSimilarityTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        LOG.debug("trying to clean existing similarities in domain: " + domainUri);

        helper.getSimilarityBuilder().delete(domainUri);

    }


}
