/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.eventbus;

import org.librairy.model.Event;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Resource;
import org.librairy.model.modules.BindingKey;
import org.librairy.model.modules.EventBus;
import org.librairy.model.modules.EventBusSubscriber;
import org.librairy.model.modules.RoutingKey;
import org.librairy.modeler.lda.services.SimilarityService;
import org.librairy.storage.UDM;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class DealsWithPartEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(DealsWithPartEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    SimilarityService similarityService;

    @Autowired
    UnifiedColumnRepository columnRepository;

    @Autowired
    UDM udm;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(Relation.Type.DEALS_WITH_FROM_PART, Relation.State.CREATED),
                "lda-modeler-deals-with-part");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.info("New topics distribution from part event received: " + event);
        try{
            Relation relation   = event.to(Relation.class);
            String topicUri     = relation.getEndUri();

            // Schedule discover similarities
            String domainUri = columnRepository.findBy(Relation.Type.EMERGES_IN, "topic", topicUri).iterator().next().getEndUri();
            similarityService.discover(domainUri, Resource.Type.PART, 10000);
            
        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
