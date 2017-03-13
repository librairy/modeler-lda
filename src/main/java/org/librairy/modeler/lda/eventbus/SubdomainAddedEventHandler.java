/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
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
import org.librairy.modeler.lda.cache.DelayCache;
import org.librairy.modeler.lda.services.SubdomainShapingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class SubdomainAddedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(SubdomainAddedEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    SubdomainShapingService service;

    @Autowired
    DelayCache delayCache;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of("subdomain.added"), "modeler.lda.subdomain.added");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {

        LOG.debug("Subdomain added event received: " + event);
        try{
            Resource resource = event.to(Resource.class);

            Long delay = delayCache.getDelay(resource.getUri());

            service.shape(resource.getUri(), delay);

        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
