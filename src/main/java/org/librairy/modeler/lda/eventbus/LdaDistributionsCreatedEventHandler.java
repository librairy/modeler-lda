/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.eventbus;

import org.librairy.boot.model.Event;
import org.librairy.boot.model.modules.BindingKey;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.EventBusSubscriber;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.services.ParallelExecutorService;
import org.librairy.modeler.lda.tasks.LDADistributionsTask;
import org.librairy.modeler.lda.tasks.LDASimilarityTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class LdaDistributionsCreatedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(LdaDistributionsCreatedEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    ModelingHelper helper;

    private ParallelExecutorService executor;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(LDADistributionsTask.ROUTING_KEY_ID), "modeler.lda.distributions.created");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
        executor = new ParallelExecutorService();
    }

    @Override
    public void handle(Event event) {
//        LOG.info("lda distributions created event received: " + event);
//        try{
//            String domainUri = event.to(String.class);
//
//            executor.execute(domainUri, 1000, new LDASimilarityTask(domainUri,helper) );
//
//        } catch (Exception e){
//            // TODO Notify to event-bus when source has not been added
//            LOG.error("Error scheduling annotations in domain: " + event, e);
//        }
    }
}
