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
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDATrainingTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDATrainingTask.class);

    public static final String ROUTING_KEY_ID = "lda.model.trained";

    private final ModelingHelper helper;

    private final String domainUri;

    public LDATrainingTask(String domainUri, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        helper.getSparkHelper().execute(() -> {
            try{
                LOG.info("Prepare workspace for domain: " + domainUri);
                helper.getWorkspaceBuilder().initialize(domainUri);

                LOG.info("creating a corpus to build a topic model in domain: " + domainUri);
                Corpus corpus = helper.getCorpusBuilder().build(domainUri, Arrays.asList(new Resource.Type[]{Resource.Type.ITEM}));

                // Train a Topic Model based on Corpus
                LOG.info("training the model ..");
                helper.getLdaBuilder().build(corpus);

                helper.getEventBus().post(Event.from(domainUri), RoutingKey.of(ROUTING_KEY_ID));
            }catch (Exception e){
                if (e instanceof InterruptedException) LOG.warn("Execution canceled");
                else LOG.error("Error on execution", e);
            }
        });

    }


}
