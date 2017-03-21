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
import org.librairy.modeler.lda.cache.DomainCache;
import org.librairy.modeler.lda.cache.ItemCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class ItemUpdated implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(ItemUpdated.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    DomainCache domainCache;

    @Autowired
    ItemCache itemCache;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(Resource.Type.ITEM, Resource.State.UPDATED ), "modeler.lda.item.updated");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.debug("Item updated: " + event);
        try{
            Resource resource = event.to(Resource.class);

            // update domains containing item
            domainCache.getDomainsFrom(resource.getUri())
                    .forEach(domain ->{
                            itemCache.updateItem(domain.getUri(), resource.getUri());
                        }
                    );

        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}