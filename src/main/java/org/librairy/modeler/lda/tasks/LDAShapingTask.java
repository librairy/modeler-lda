/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDAShapingTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDAShapingTask.class);

    public static final String ROUTING_KEY_ID = "lda.shapes.created";

    private final ModelingHelper helper;

    private final String domainUri;

    public LDAShapingTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        try{
            final ComputingContext context = helper.getComputingHelper().newContext("lda.shapes."+ URIGenerator.retrieveId(domainUri));
            helper.getComputingHelper().execute(context, () -> {
                try{
                    // Create corpus
                    Corpus corpus = helper.getCorpusBuilder().build(context, domainUri, Arrays.asList(new Resource.Type[]{Resource.Type.ITEM, Resource.Type.PART}));

                    // Load existing model
                    String domainId = URIGenerator.retrieveId(domainUri);
                    TopicModel model = helper.getLdaBuilder().load(context, domainId);

                    // Use of existing vocabulary
                    corpus.setCountVectorizerModel(model.getVocabModel());

                    // Calculate topic distributions for Items and Parts
                    helper.getDealsBuilder().build(context, corpus,model);

                    helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(ROUTING_KEY_ID));

                } catch (Exception e){
                    if (e instanceof InterruptedException){ LOG.info("Execution interrupted during process.");}
                    else LOG.error("Error scheduling a new topic model for Items from domain: " + domainUri, e);
                }
            });
        } catch (InterruptedException e) {
            LOG.info("Execution interrupted.");
        }



    }


}
