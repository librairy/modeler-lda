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
import org.librairy.modeler.lda.builder.WorkspaceBuilder;
import org.librairy.modeler.lda.services.ModelingService;
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
public class DomainDeletedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(DomainDeletedEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    WorkspaceBuilder workspaceBuilder;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(Resource.Type.DOMAIN, Resource.State.DELETED),
                "modeler.lda.domain.updated");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        try{
            Resource resource = event.to(Resource.class);

            LOG.info("Deleting domain tables: " + resource.getUri());
            workspaceBuilder.destroy(resource.getUri());

        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for change in a domain: " + event, e);
        }
    }
}
