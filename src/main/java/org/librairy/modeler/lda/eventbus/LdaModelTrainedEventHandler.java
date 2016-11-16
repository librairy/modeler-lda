/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.eventbus;

import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.modules.BindingKey;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.EventBusSubscriber;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.modeler.lda.builder.CorpusBuilder;
import org.librairy.modeler.lda.builder.DealsBuilder;
import org.librairy.modeler.lda.builder.LDABuilder;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class LdaModelTrainedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(LdaModelTrainedEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    CorpusBuilder corpusBuilder;

    @Autowired
    LDABuilder ldaBuilder;

    @Autowired
    DealsBuilder dealsBuilder;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of("lda.model.trained"), "modeler.lda.model.trained");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.info("lda model trained event received: " + event);
        try{
            String domainUri = event.to(String.class);

            // Create corpus
            Corpus corpus = corpusBuilder.build(domainUri, Arrays.asList(new Resource.Type[]{Resource.Type.ITEM, Resource.Type.PART}));

            // Load existing model
            String domainId = URIGenerator.retrieveId(domainUri);
            TopicModel model = ldaBuilder.load(domainId);

            // Use of existing vocabulary
            corpus.setCountVectorizerModel(model.getVocabModel());

            // Calculate topic distributions for Items and Parts
            dealsBuilder.build(corpus,model);

            eventBus.post(Event.from(domainUri), RoutingKey.of("lda.shapes.created"));

        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
