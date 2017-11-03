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
import org.librairy.modeler.lda.services.ModelingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created on 09/09/16:
 *
 * @author cbadenes
 */
@Component
public class DomainPendingEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(DomainPendingEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    ModelingService modelingService;

    @Autowired
    DelayCache delayCache;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of("domain.pending"), "modeler.lda.domain.pending");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.debug("Domain to be updated event received: " + event);
        try{
            Resource resource = event.to(Resource.class);

            modelingService.enablePendingModelingFor(resource.getUri());
            Long delay = delayCache.getDelay(resource.getUri());
            modelingService.train(resource.getUri(), delay);

        } catch (Exception e){
            LOG.error("Error scheduling a new W2V model in domain: " + event, e);
        }
    }
}
