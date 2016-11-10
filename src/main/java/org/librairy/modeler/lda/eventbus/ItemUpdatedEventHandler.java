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
import org.librairy.modeler.lda.services.ModelingService;
import org.librairy.storage.UDM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class ItemUpdatedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(ItemUpdatedEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    ModelingService modelingService;

    @Value("#{environment['LIBRAIRY_LDA_EVENT_DELAY']?:${librairy.lda.event.delay}}")
    protected Long delay;

    @Autowired
    UDM udm;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(Resource.Type.ITEM, Resource.State.UPDATED),
                "modeler.lda.item.updated");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.debug("Item bundled event received: " + event);
        try{
            Resource resource = event.to(Resource.class);

            udm.find(Resource.Type.DOMAIN).from(Resource.Type.ITEM,resource.getUri()).forEach(domain -> modelingService.train(domain.getUri(),delay));

        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
