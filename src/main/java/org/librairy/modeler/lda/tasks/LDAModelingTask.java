/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.exceptions.EmptyResultException;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDAModelingTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDAModelingTask.class);

    private final ModelingHelper helper;

    private final String domainUri;

    public LDAModelingTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        LOG.debug("trying to use an existing topic model to discover topic distributions in domain: " + domainUri);

        try{
            // Create corpus
            Corpus corpus = helper.getCorpusBuilder().build(domainUri, Resource.Type.PART);

            // Load existing model
            String domainId = URIGenerator.retrieveId(domainUri);
            TopicModel model = helper.getLdaBuilder().load(domainId);

            // Use of existing vocabulary
            corpus.setCountVectorizerModel(model.getVocabModel());

            // Load topic URIs
            Map<String, String> registry = helper.getTopicsBuilder().composeRegistry(model);

            // Calculate topic distributions for Items
            helper.getDealsBuilder().build(corpus,model,registry);
        }catch (EmptyResultException e){
            LOG.info(e.getMessage());
        }

    }


}
