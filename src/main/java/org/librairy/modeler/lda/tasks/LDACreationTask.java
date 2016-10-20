/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.google.common.collect.Iterators;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDACreationTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDACreationTask.class);

    private final ModelingHelper helper;

    private final String domainUri;

    public LDACreationTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        LOG.info("creating a corpus to build a topic model in domain: " + domainUri);
        Corpus corpus = helper.getCorpusBuilder().build(domainUri, Resource.Type.ITEM);

        // Train a Topic Model based on Corpus
        LOG.info("training the model ..");
        TopicModel model = helper.getLdaBuilder().build(corpus);

        // Persist the model on database
        LOG.info("persisting the model on database ..");
        Map<String, String> registry = helper.getTopicsBuilder().persist(model);

        // Calculate topic distributions for Items
        LOG.info("calculating the topics distribution for each textual item ..");
        helper.getDealsBuilder().build(corpus,model,registry);

        LOG.info("LDA model created and stored successfully");
    }


}
