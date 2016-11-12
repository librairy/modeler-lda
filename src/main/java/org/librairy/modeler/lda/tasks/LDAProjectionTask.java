/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import org.librairy.model.Event;
import org.librairy.model.domain.resources.Resource;
import org.librairy.model.modules.RoutingKey;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDAProjectionTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDAProjectionTask.class);

    private final ModelingHelper helper;

    private final String domainUri;

    public LDAProjectionTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

    }


}
